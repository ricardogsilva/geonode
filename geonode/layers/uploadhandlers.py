# -*- coding: utf-8 -*-
#########################################################################
#
# Copyright (C) 2017 OSGeo
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
#########################################################################

"""Handlers for dealing with uploaded files."""

from collections import namedtuple
import logging
import os.path
from uuid import uuid1
import zipfile

from django import forms
from django.conf import settings
from django.core.files import File
from django.db import transaction
from django.template.defaultfilters import slugify
from django.utils.translation import ugettext_lazy as _
from lxml import etree

from ..base.models import SpatialRepresentationType
from ..base.models import TopicCategory
from . import metadata
from . import models
from . import utils

logger = logging.getLogger(__name__)


ShapefileAux = namedtuple("ShapefileAux", [
    "extension",
    "mandatory"
])


def get_upload_handler(form_cleaned_data):
    base_file = form_cleaned_data["base_file"]
    extension = os.path.splitext(base_file.name)[-1].replace(".", "").lower()
    if zipfile.is_zipfile(base_file):
        if extension == "kmz":
            handler_class = KmzHandler
        elif extension == "zip":
            handler_class = ZippedFileHandler
        else:
            raise NotImplementedError
    elif extension == "kml":
        handler_class = KmlHandler
    elif extension == "shp":
        handler_class = ShapefileHandler
    elif extension == "asc":
        handler_class = AsciiHandler
    elif extension in ("tif", "tiff", "geotif", "geotiff"):
        handler_class = GeotiffHandler
    else:
        raise forms.ValidationError(
            _("Unsupported file type %(value)s"),
            params={"value": base_file}
        )
    logger.debug("handler_class: {}".format(handler_class))
    return handler_class


def create_geonode_layer(handler, user, temporary_directory, permissions,
                         workspace, preserve_metadata=False):
    """Create a new layer in geonode's database"""
    saved_files = handler.save_files(temporary_directory)
    bbox = handler.get_bbox(saved_files)
    if saved_files.get("xml") is not None:
        meta = metadata.retrieve_xml_metadata(saved_files["xml"])
        resolved, unresolved= utils.resolve_regions(meta.pop("regions"))
        keywords = unresolved
    else:
        meta = None
        keywords = []
    layer = handler.instantiate_geonode_layer(
        user,
        bbox,
        store=handler.get_store_name(saved_files),
        workspace=workspace,
        additional_metadata=meta,
        preserve_uploaded_metadata=preserve_metadata,
    )
    layer.set_permissions(permissions)
    # TODO: Make sure that layer's name is unique
    if getattr(settings, 'NLP_ENABLED', False):
        additional_info = _extract_additional_info(
            layer.title, layer.abstract, layer.purpose)
    layer.full_clean()
    with transaction.atomic():
        session = handler.create_geonode_upload_session(user, **saved_files)
        layer.upload_session = session
        layer.save()
    return layer


def _extract_additional_info(title, abstract, purpose):
    regions = []
    keywords = []
    try:
        from geonode.contrib.nlp.utils import nlp_extract_metadata_dict
        nlp_metadata = nlp_extract_metadata_dict({
            'title': title,
            'abstract': abstract,
            'purpose': purpose
        })
        if nlp_metadata:
            regions.extend(nlp_metadata.get('regions', []))
            keywords.extend(nlp_metadata.get('keywords', []))
    except BaseException:
        print("NLP extraction failed.")
    return regions, keywords


class BaseHandler(object):
    """Base class for upload handlers.

    Reimplement to add custom file types to geonode.

    """

    def __init__(self, form_cleaned_data, form_files):
        self.title = utils.get_uploaded_layer_title(
            form_cleaned_data["base_file"].name,
            layer_title=form_cleaned_data["layer_title"],
        )
        self.abstract = form_cleaned_data["abstract"] or _(
            "No abstract provided")
        self.charset = form_cleaned_data["charset"] or "utf-8"

    def instantiate_geonode_layer(self, owner, bbox, workspace, store,
                                  additional_metadata=None,
                                  preserve_uploaded_metadata=False):
        """Returns a new Layer instance but does not save it in the db."""
        raise NotImplementedError

    def get_bbox(self, saved_files):
        raise NotImplementedError

    def get_store_name(self, saved_files):
        raise NotImplementedError

    def save_files(self, destination_dir):
        raise NotImplementedError

    def upload_files_to_geoserver(self):
        raise NotImplementedError


class GeotiffHandler(BaseHandler):

    def __init__(self, form_cleaned_data, form_files):
        super(GeotiffHandler, self).__init__(
            form_cleaned_data, form_files)
        self.resource = form_cleaned_data["base_file"]

    def save_files(self, destination_dir):
        return save_django_files([self.resource], destination_dir)


class AsciiHandler(BaseHandler):

    def __init__(self, form_cleaned_data, form_files):
        super(AsciiHandler, self).__init__(
            form_cleaned_data, form_files)
        self.resource = form_cleaned_data["base_file"]

    def save_files(self, destination_dir):
        return save_django_files([self.resource], destination_dir)


class ZippedFileHandler(BaseHandler):

    def __init__(self, form_cleaned_data, form_files):
        super(ZippedFileHandler, self).__init__(
            form_cleaned_data, form_files)
        self.zip_file = form_cleaned_data["base_file"]
        with zipfile.ZipFile(self.zip_file) as zip_handler:
            zip_contents = zip_handler.namelist()
        self.resources = _validate_shapefile_components(zip_contents)

    def save_files(self, destination_dir):
        return save_zipped_file(self.zip_file, destination_dir)


class KmzHandler(BaseHandler):

    def __init__(self, form_cleaned_data, form_files):
        super(KmzHandler, self).__init__(
            form_cleaned_data, form_files)
        self.kmz_file = form_cleaned_data["base_file"]
        if not zipfile.is_zipfile(self.kmz_file):
            raise forms.ValidationError(_("Invalid kmz file detected"))
        with zipfile.ZipFile(self.kmz_file) as zip_handler:
            zip_contents = zip_handler.namelist()
            kml_files = [i for i in zip_contents if i.lower().endswith(".kml")]
            try:
                self.kml_zip_path = kml_files[0]
                kml_bytes = zip_handler.read(self.kml_zip_path)
            except IndexError:
                raise forms.ValidationError(
                    _("Could not find any kml files inside the uploaded kmz"))
        if len(kml_files) > 1:
            raise forms.ValidationError(
                _("Only one kml file per kmz is allowed"))
        other_components = [
            i for i in zip_contents if not i.lower().endswith(".kml")]
        _validate_kml(kml_bytes, other_components)

    def save_files(self, destination_dir):
        return save_zipped_file(self.kmz_file, destination_dir)


class ShapefileHandler(BaseHandler):

    def __init__(self, form_cleaned_data, form_files):
        super(ShapefileHandler, self).__init__(
            form_cleaned_data, form_files)
        file_names = [f.name for f in form_files.itervalues()]
        resources = _validate_shapefile_components(file_names)
        self.files = {}
        for extension, path in resources.items():
            for django_file in form_files.itervalues():
                if django_file.name == path:
                    self.files[extension] = django_file
                    break
            else:
                raise RuntimeError(
                    "Could not find django file for {}".format(path))
        self.store_type = "dataStore"

    def save_files(self, destination_dir):
        return save_django_files(self.files.itervalues(), destination_dir)

    def get_bbox(self, saved_files):
        return utils.get_bbox(saved_files["shp"])

    def get_store_name(self, saved_files):
        return self.title

    def instantiate_geonode_layer(self, owner, bbox, workspace, store,
                                  additional_metadata=None,
                                  preserve_uploaded_metadata=False):
        """Returns a new Layer instance but does not save it in the db."""
        x0, x1, y0, y1 = bbox
        layer = models.Layer(
            # upload_session="",  # add upload_session later
            title=self.title,
            abstract=self.abstract,
            owner=owner,
            charset=self.charset,
            bbox_x0=x0,
            bbox_x1=x1,
            bbox_y0=y0,
            bbox_y1=y1,
            name=self.title,
            storeType=self.store_type,
            metadata_uploaded_preserve=preserve_uploaded_metadata,
            workspace=workspace,
            store=store,
        )
        if additional_metadata:
            meta = additional_metadata.copy()
            layer.metadata_uploaded = True
            if preserve_uploaded_metadata:
                layer.uuid = meta.get("uuid")
                layer.metadata_xml = meta.get("metadata_xml")
            meta.pop("uuid")
            meta.pop("metadata_xml")
            for remaining_key, remaining_value in meta.items():
                setattr(layer, remaining_key, remaining_value)
        else:
            layer.uuid = str(uuid1())
        return layer


class KmlHandler(BaseHandler):

    def __init__(self, form_cleaned_data, form_files):
        super(KmlHandler, self).__init__(
            form_cleaned_data, form_files)
        kml_file = form_cleaned_data["base_file"]
        other_files = [
            i.name for i in form_files.itervalues() if i is not kml_file]
        kml_file.seek(0)
        kml_bytes = kml_file.read()  # TODO: use kml_file.chunks instead?
        result = _validate_kml(kml_bytes, other_files)
        self.files = {
            "kml": kml_file,
        }
        if len(result) > 1:
            overlay_name = result[1]
            ground_overlay = [
                i for i in other_files if i.name == overlay_name][0]
            self.files[_get_extension(overlay_name)] = ground_overlay

    def save_files(self, destination_dir):
        return save_django_files(self.files.itervalues(), destination_dir)


def _get_extension(file_fragment):
    return os.path.splitext(file_fragment)[-1].replace(".", "")


def _get_kml_doc(kml_bytes):
    """Parse and return an etree element with the kml file's content"""
    kml_doc = etree.fromstring(
        kml_bytes,
        parser=etree.XMLParser(resolve_entities=False)
    )
    ns = kml_doc.nsmap.copy()
    ns["kml"] = ns.pop(None)
    return kml_doc, ns


def save_django_files(files, destination_dir):
    result = {}
    for django_file in files:
        target_path = os.path.join(destination_dir, django_file.name)
        logger.debug("Saving {!r} in {}...".format(
            django_file.name, destination_dir))
        with open(target_path, "wb") as destination:
            for chunk in django_file.chunks():
                destination.write(chunk)
        result[_get_extension(django_file.name)] = target_path
    return result


def save_zipped_file(zipped_file, destination_dir):
    result = {}
    with zipfile.ZipFile(zipped_file) as zip_handler:
        for member in zip_handler.namelist():
            logger.debug("Extracting {} to {}...".format(
                member, destination_dir))
            zip_handler.extract(member, destination_dir)
            result[_get_extension(member)] = os.path.join(
                destination_dir, member)
    return result


def _validate_ground_overlay_kml(ground_overlay_element, namespaces,
                                 possible_files):
    """Validates that a kml with a ground overlay type has the correct image

    :arg ground_overlay_element: The kml:GroundOverlay element of the kml file
    :type ground_overlay_element: etree._Element

    """

    icon_path = ground_overlay_element.xpath(
        "kml:Icon/kml:href/text()", namespaces=namespaces)[0]
    if icon_path not in possible_files:
        raise forms.ValidationError(
            _("Ground overlay image declared in kml file cannot be found"))
    return icon_path


def _validate_kml(kml_bytes, other_files):
    kml_doc, namespaces = _get_kml_doc(kml_bytes)
    ground_overlays = kml_doc.xpath(
        "//kml:GroundOverlay", namespaces=namespaces)
    logger.debug("ground_overlays: {}".format(ground_overlays))
    if len(ground_overlays) > 1:
        raise forms.ValidationError(
            _("kml files with more than one GroundOverlay are not supported"))
    elif len(ground_overlays) == 1:
        image_path = _validate_ground_overlay_kml(
            ground_overlays[0], namespaces, other_files)
        result = kml_doc, image_path
    else:
        result = kml_doc
    return result


def _validate_shapefile_components(possible_files):
    """Validates that a shapefile can be loaded from the input file paths

    :arg possible_files: Remaining form upload contents
    :type possible_files: list

    """

    # TODO: Use a more robust way of determining if it is shapefile
    shp_files = [i for i in possible_files if i.lower().endswith(".shp")]
    if len(shp_files) > 1:
        raise forms.ValidationError(_("Only one shapefile per zip is allowed"))
    shape_component = shp_files[0]
    base_name = os.path.splitext(os.path.basename(shape_component))[0]
    components = {
        "shp": shape_component
    }
    shapefile_additional = [
        ShapefileAux(extension="dbf", mandatory=True),
        ShapefileAux(extension="shx", mandatory=True),
        ShapefileAux(extension="prj", mandatory=False),
        ShapefileAux(extension="xml", mandatory=False),
        ShapefileAux(extension="sld", mandatory=False),
    ]
    for additional_component in shapefile_additional:
        for path in possible_files:
            additional_name = os.path.splitext(os.path.basename(path))[0]
            matches_main_name = additional_name == base_name
            found_component = path.endswith(additional_component.extension)
            if found_component and matches_main_name:
                components[additional_component.extension] = path
                break
        else:
            if additional_component.mandatory:
                raise forms.ValidationError(
                    "Could not find {!r} file, which is mandatory for "
                    "shapefile uploads".format(
                        additional_component.extension)
                )
    logger.debug("shapefile components: {}".format(components))
    return components