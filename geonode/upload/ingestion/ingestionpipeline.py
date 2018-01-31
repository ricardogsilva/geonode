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

"""Tools for dealing with the geoserver ingestion pipeline"""

import logging
import os.path

from osgeo import gdal
from osgeo import osr

logger = logging.getLogger(__name__)



def analyze_paths(paths):
    """Extract data features from the input paths

    The extracted features can be used for selecting which transformation
    profile is more appropriate for the input data.

    """
    features = []
    for path in paths:
        features.append(_extract_features(path))
    return features


def select_transform(extracted_features):
    """Select the most suitable transformation chain"""
    return []


def _extract_features(path):
    handler = gdal.Open(path)
    projection_wkt = handler.GetProjection()
    spatial_ref = osr.SpatialReference()
    spatial_ref.ImportFromWkt(projection_wkt)
    spatial_ref.AutoIdentifyEPSG()
    geotransform = handler.GetGeoTransform()
    features = {
        "path": path,
        "basename": os.path.basename(path),
        "extension": os.path.splitext(path)[-1][1:],
        "num_bands": handler.RasterCount,
        "cols": handler.RasterXSize,
        "rows": handler.RasterYSize,
        "minx": geotransform[0],
        "maxy": geotransform[3],
        "epsg_code": spatial_ref.GetAuthorityCode(None),
    }
    # get features from each band -> keep the dict flat!
    for band_idx in range(1, features["num_bands"]+1):
        band = handler.GetRasterBand(band_idx)
        band_id = "band_{}".format(band_idx)
        features["{}_datatype".format(band_id)] = _get_data_type(band.DataType)
    return features


def _get_data_type(gdal_data_type):
    return {
        1: "byte",
    }.get(gdal_data_type)

