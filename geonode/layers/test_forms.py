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

"""Unit tests for geonode.layers.forms"""

from io import BytesIO
import zipfile

from django.test import TestCase
from django.core.files.uploadedfile import SimpleUploadedFile
from django.core.files.uploadedfile import InMemoryUploadedFile

from geonode.layers import forms


def _write_kmz(buffer_, kml_file, image_file):
    with zipfile.ZipFile(buffer_, "w") as kmz_file:
        # kmz_file = zipfile.ZipFile(buffer, "w")
        kmz_file.writestr(kml_file.name, kml_file.read())
        kmz_file.writestr(image_file.name, image_file.read())
    # kmz_file.close()
    return buffer_


class NewLayerUploadFormTestCase(TestCase):

    def setUp(self):
        self.kml_file_name = "this.kml"
        self.kmz_file_name = "that.kmz"
        self.kml_image_file_name = "my-image.png"
        self.kml_contents = """
        <?xml version="1.0" encoding="UTF-8"?>
        <kml xmlns="http://earth.google.com/kml/2.1">
            <Document>
                <name>CSR5r3_annual</name>
                <GroundOverlay id="groundoverlay">
                    <name>CSR5r3_annual</name>
                    <description><![CDATA[]]></description>
                    <color>ffffffff</color>
                    <visibility>1</visibility>
                    <extrude>0</extrude>
                    <Icon>
                        <href>{}</href>
                        <viewBoundScale>1</viewBoundScale>
                    </Icon>
                    <LatLonBox>
                        <north>70.000000</north>
                        <south>-60.500000</south>
                        <east>180.000000</east>
                        <west>-180.000000</west>
                        <rotation>0.0000000000000000</rotation>
                    </LatLonBox>
                    </GroundOverlay>
            </Document>
        </kml>""".format(self.kml_image_file_name).strip()
        image_contents = b"nothing really"
        self.kml_file = SimpleUploadedFile(
            self.kml_file_name, self.kml_contents)
        self.kml_image_file = SimpleUploadedFile(
            self.kml_image_file_name, image_contents)
        buffer_ = BytesIO()
        _write_kmz(buffer_, self.kml_file, self.kml_image_file)
        buffer_.seek(0)
        self.kml_file.seek(0)
        self.kml_image_file.seek(0)
        self.kmz_file = SimpleUploadedFile(self.kmz_file_name, buffer_.read())

    def test_form_is_valid_with_ground_overlay_kml(self):
        kml_file = SimpleUploadedFile(self.kml_file_name, self.kml_contents)
        f = forms.NewLayerUploadForm(
            data={
                "permissions": "{}",
            },
            files={
                "base_file": kml_file,
                "png_file": self.kml_image_file
            }
        )
        is_valid = f.is_valid()
        print("validation errors: {}".format(f.errors))
        self.assertTrue(is_valid)

    def test_form_is_valid_with_ground_overlay_kmz(self):
        f = forms.NewLayerUploadForm(
            data={
                "permissions": "{}",
            },
            files={
                "base_file": self.kmz_file,
            }
        )
        is_valid = f.is_valid()
        print("validation errors: {}".format(f.errors))
        self.assertTrue(f.is_valid())
