# -*- coding: utf-8 -*-
#########################################################################
#
# Copyright (C) 2018 OSGeo
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

"""Tools for performing validation of uploaded spatial files."""

from __future__ import division
import re

import os
import os.path
import logging
import zipfile

from django.utils.translation import ugettext_lazy as _

from .utils import get_kml_doc

logger = logging.getLogger(__name__)


KML_IMAGE_EXTENSIONS = {
    "tif": ["tif", "tiff", "geotif", "geotiff"],
    "jpg": ["jpg", "jpeg"],
    "png": ["png"],
    "gif": ["gif"],
}

SHAPEFILE_COMPONENTS = {
    "shp": True,
    "dbf": True,
    "shx": True,
    "prj": False,
    "xml": False,
    "sld": False,
}


def clean_name(name, regex=r"(^[^a-zA-Z\._]+)|([^a-zA-Z\._0-9]+)",
               replace="_"):
    """Replaces a string that matches the regex with the replacement."""
    regex = re.compile(regex)
    if name[0].isdigit():
        name = replace + name
    return regex.sub(replace, name)


def ensure_safe_file_name(file_name):
    dirname, base_name = os.path.split(file_name)
    safe = clean_name(base_name)
    if safe != base_name:
        safe = os.path.join(dirname, safe)
        os.rename(file_name, safe)
        result = safe
    else:
        result = file_name
    return result


def extract_zip(zip_handler, destination):
    file_names = zip_handler.namelist()
    zip_handler.extractall(destination)
    return [os.path.join(destination, p) for p in file_names]


def get_file_extension(name):
    return os.path.splitext(name)[1][1:].lower()


def get_upload_handler(uploaded_files):
    extensions = [get_file_extension(p.name) for p in uploaded_files]
    if "zip" in extensions:
        zip_file = _get_file_by_extension("zip", uploaded_files)
        handler = ZipHandler(zip_file)
    elif "kmz" in extensions:
        kmz_file = _get_file_by_extension("kmz", uploaded_files)
        handler = KmzHandler(kmz_file)
    elif "shp" in extensions:
        handler = ShapefileHandler(uploaded_files)
    elif "kml" in extensions:
        handler = KmlHandler(uploaded_files)
    else:
        handler = GenericHandler(uploaded_files)
    return handler


def look_for_kml_image_file(file_names):
    result = None
    for file_name in file_names:
        file_extension = get_file_extension(file_name)
        for extension, aliases in KML_IMAGE_EXTENSIONS.items():
            if file_extension in aliases:
                result = file_name
                break
        if result is not None:
            break
    return result


def validate_kml_ground_overlay(kml_bytes, image_filepath):
    kml_doc, namespaces = get_kml_doc(kml_bytes)
    ground_overlays = kml_doc.xpath(
        "//kml:GroundOverlay", namespaces=namespaces)
    if len(ground_overlays) > 1:
        raise RuntimeError(
            _("kml files with more than one GroundOverlay are "
              "not supported")
        )
    elif len(ground_overlays) == 1:
        try:
            image_path = ground_overlays[0].xpath(
                "kml:Icon/kml:href/text()",
                namespaces=namespaces
            )[0].strip()
        except IndexError:
            image_path = ""
        logger.debug("image_path: {}".format(image_path))
        if image_path != image_filepath:
            raise RuntimeError(
                _("Ground overlay image declared in kml file cannot "
                  "be found")
            )


def write_uploaded_files_to_disk(target_dir, *files):
    written_paths = []
    for django_file in files:
        if zipfile.is_zipfile(django_file):
            with zipfile.ZipFile(django_file) as zip_handler:
                extracted = extract_zip(zip_handler, target_dir)
                written_paths.extend(extracted)
        else:
            path = os.path.join(target_dir, django_file.name)
            with open(path, 'wb') as fh:
                for chunk in django_file.chunks():
                    fh.write(chunk)
            written_paths = path
    result = []
    for path in written_paths:
        result.append(ensure_safe_file_name(path))
    return result


class UploadHandler(object):

    def validate_files(self):
        raise NotImplementedError

    def write_files(self, destination):
        raise NotImplementedError


class GenericHandler(UploadHandler):

    def __init__(self, files):
        self.uploaded_files = files

    def validate_files(self):
        pass

    def write_files(self, destination):
        return write_uploaded_files_to_disk(destination, *self.uploaded_files)


class ZipHandler(UploadHandler):

    def __init__(self, zip_file):
        if not zipfile.is_zipfile(zip_file):
            raise RuntimeError(_("Invalid zip file detected"))
        self.zip_file = zip_file

    def validate_files(self):
        shapefile_components = {}
        with zipfile.ZipFile(self.zip_file) as zip_handler:
            zip_contents = zip_handler.namelist()
        for extension_name in SHAPEFILE_COMPONENTS:
            for item in zip_contents:
                item_extension = get_file_extension(item)
                if item_extension == extension_name:
                    shapefile_components[extension_name] = item
        try:
            shp_name = os.path.splitext(shapefile_components["shp"])[0]
        except KeyError:
            raise RuntimeError("Could not find shp file within the zip")
        for extension, mandatory in SHAPEFILE_COMPONENTS.items():
            zip_item = shapefile_components.get(extension)
            if zip_item is None and mandatory:
                raise RuntimeError(
                    "Could not find {!r} file, which is mandatory for "
                    "shapefiles".format(extension)
                )
            elif zip_item is not None:
                file_base = os.path.splitext(os.path.basename(zip_item))[0]
                if file_base != shp_name:
                    raise RuntimeError(
                        "shp file {!r} and {} file {!r} do not match".format(
                            shp_name, extension, file_base)
                    )

    def write_files(self, destination):
        return write_uploaded_files_to_disk(destination, self.zip_file)


class KmzHandler(UploadHandler):

    def __init__(self, kmz_file):
        if not zipfile.is_zipfile(kmz_file):
            raise RuntimeError(_("Invalid kmz file detected"))
        self.kmz_file = kmz_file

    def validate_files(self):
        with zipfile.ZipFile(self.kmz_file) as zip_handler:
            contents = zip_handler.namelist()
            image_file_name = look_for_kml_image_file(contents)
            try:
                kml_file = [
                    i for i in contents if i.lower().endswith(".kml")][0]
            except IndexError:
                raise RuntimeError(
                    _("Could not find any kml files inside the uploaded kmz"))
            if image_file_name is not None:
                kml_bytes = zip_handler.read(kml_file)
                validate_kml_ground_overlay(kml_bytes, image_file_name)

    def write_files(self, destination):
        return write_uploaded_files_to_disk(destination, self.kmz_file)


class KmlHandler(UploadHandler):

    def __init__(self, files):
        self.kml_file = [f for f in files if f.name.lower().endswith("kml")][0]
        # self.image_file = look_for_kml_image_file(files)
        image_name = look_for_kml_image_file([f.name for f in files])
        try:
            self.image_file = [f for f in files if f.name == image_name][0]
        except IndexError:
            self.image_file = None

    def validate_files(self):
        """Validate uploaded KML file and a possible image companion file

        KML files that specify vectorial data typers are uploaded standalone.
        However, if the KML specifies a GroundOverlay type (raster) they are
        uploaded together with a raster file.

        """

        if self.image_file is not None:
            self.kml_file.seek(0)
            kml_bytes = self.kml_file.read()
            validate_kml_ground_overlay(kml_bytes, self.image_file.name)

    def write_files(self, destination):
        to_write = [self.kml_file]
        if self.image_file is not None:
            to_write.append(self.image_file)
        return write_uploaded_files_to_disk(destination, *to_write)


class ShapefileHandler(UploadHandler):

    def __init__(self, files):
        self.shapefile_components = {}
        for extension_name in SHAPEFILE_COMPONENTS:
            for django_file in files:
                extension = os.path.splitext(django_file.name)[1][1:].lower()
                if extension == extension_name:
                    self.shapefile_components[extension] = django_file

    def validate_files(self):
        """Validates that a shapefile can be loaded from the input file paths

        :raises: RuntimeError

        """

        try:
            shp_file = self.shapefile_components["shp"]
        except KeyError:
            raise RuntimeError("Could not find shp file")
        shp_name = os.path.splitext(os.path.basename(shp_file.name))[0]
        for component_extension, mandatory in SHAPEFILE_COMPONENTS.items():
            django_file = self.shapefile_components.get(component_extension)
            if django_file is None and mandatory:
                raise RuntimeError(
                    "Could not find {!r} file, which is mandatory for "
                    "shapefiles".format(component_extension)
                )
            elif django_file is not None:
                file_base = os.path.splitext(
                    os.path.basename(django_file.name))[0]
                if file_base != shp_name:
                    raise RuntimeError(
                        "shp file {!r} and {} file {!r} do not match".format(
                            shp_name, component_extension, file_base)
                    )

    def write_files(self, destination):
        to_write = list(self.shapefile_components.values())
        return write_uploaded_files_to_disk(destination, *to_write)


def _get_file_by_extension(extension, files):
    for django_file in files:
        file_extension = get_file_extension(django_file.name)
        if file_extension == extension:
            result = django_file
            break
    else:
        result = None
    return result
