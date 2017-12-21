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
import zipfile

from django import forms
from django.utils.translation import ugettext_lazy as _
from lxml import etree

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
            handler_class = KmzUploadHandler
        elif extension == "zip":
            handler_class = ZippedFileUploadHandler
        else:
            raise NotImplementedError
    elif extension == "kml":
        handler_class = KmlUploadHandler
    elif extension == "shp":
        handler_class = ShapefileUploadHandler
    elif extension == "asc":
        handler_class = AsciiUploadHandler
    elif extension in ("tif", "tiff", "geotif", "geotiff"):
        handler_class = GeotiffUploadHandler
    else:
        raise forms.ValidationError(
            _("Unsupported file type %(value)s"),
            params={"value": base_file}
        )
    logger.debug("handler_class: {}".format(handler_class))
    return handler_class()


class BaseUploadHandler(object):
    """Base class for upload handlers.

    Reimplement to add custom file types to geonode.

    """

    def validate_form_fields(self, form_cleaned_data, form_files):
        raise NotImplementedError

    def save_files(self, destination_dir):
        raise NotImplementedError


class GeotiffUploadHandler(BaseUploadHandler):

    def validate_form_fields(self, form_cleaned_data, form_files):
        return form_cleaned_data["base_file"]


class AsciiUploadHandler(BaseUploadHandler):

    def validate_form_fields(self, form_cleaned_data, form_files):
        return form_cleaned_data["base_file"]



class ZippedFileUploadHandler(BaseUploadHandler):

    def validate_form_fields(self, form_cleaned_data, form_files):
        zip_handler = zipfile.ZipFile(form_cleaned_data["base_file"])
        zip_contents = zip_handler.namelist()
        return _validate_shapefile_components(zip_contents)


class KmzUploadHandler(BaseUploadHandler):

    def __init__(self):
        self.kml_doc = None
        self.ground_overlay = None

    def validate_form_fields(self, form_cleaned_data, form_files):
        kmz_file = form_cleaned_data["base_file"]
        if not zipfile.is_zipfile(kmz_file):
            raise forms.ValidationError(_("Invalid kmz file detected"))
        with zipfile.ZipFile(kmz_file) as zip_handler:
            zip_contents = zip_handler.namelist()
            kml_files = [i for i in zip_contents if i.lower().endswith(".kml")]
            try:
                kml_bytes = zip_handler.read(kml_files[0])
            except IndexError:
                raise forms.ValidationError(
                    _("Could not find any kml files inside the uploaded kmz"))
        if len(kml_files) > 1:
            raise forms.ValidationError(_("Only one kml file per kmz is allowed"))
        other_components = [
            i for i in zip_contents if not i.lower().endswith(".kml")]
        result = _validate_kml(kml_bytes, other_components)
        self.kml_doc = result[0]
        if len(result) == 2:
            self.ground_overlay = result[1]

    def save_files(self, destination_dir):
        pass


class ShapefileUploadHandler(BaseUploadHandler):

    def validate_form_fields(self, form_cleaned_data, form_files):
        file_names = [f.name for f in form_files.itervalues()]
        return _validate_shapefile_components(file_names)

    def save_files(self, destination_dir):
        pass


class KmlUploadHandler(BaseUploadHandler):

    def validate_form_fields(self, form_cleaned_data, form_files):
        kml_file = form_cleaned_data["base_file"]
        other_files = [
            i.name for i in form_files.itervalues() if i is not kml_file]
        kml_file.seek(0)
        kml_bytes = kml_file.read()  # TODO: use kml_file.chunks instead?
        return _validate_kml(kml_bytes, other_files)


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


def _get_kml_doc(kml_bytes):
    """Parse and return an etree element with the kml file's content"""
    kml_doc = etree.fromstring(
        kml_bytes,
        parser=etree.XMLParser(resolve_entities=False)
    )
    ns = kml_doc.nsmap.copy()
    ns["kml"] = ns.pop(None)
    return kml_doc, ns


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
    components = [shape_component]
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
                components.append(path)
                break
        else:
            if additional_component.mandatory:
                raise forms.ValidationError(
                    "Could not find {!r} file, which is mandatory for "
                    "shapefile uploads".format(
                        additional_component.extension)
                )
    return components