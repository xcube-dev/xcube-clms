# The MIT License (MIT)
# Copyright (c) 2024 by the xcube development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NON INFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
import logging

DATA_STORE_ID = "clms"

# CLMS API Constants
SEARCH_ENDPOINT = "@search"
DOWNLOAD_ENDPOINT = "@datarequest_post"
TASK_STATUS_ENDPOINT = "@datarequest_search"
PORTAL_TYPE = {"portal_type": "DataSet"}
ACCEPT_HEADER = {"Accept": "application/json"}
METADATA_FIELDS = "metadata_fields"
FULL_SCHEMA = "fullobjects"
CLMS_DATA_ID = "id"
DATASET_FORMAT = "distribution_format_list"
UID = "UID"
DOWNLOADABLE_FILES = "downloadable_files"
ITEMS = "items"
SPATIAL_COVERAGE = "area"
RESOLUTION = "resolution"
FILE_ID = "@id"


# CLMS API URLS
CLMS_API_AUTH = "https://land.copernicus.eu/@@oauth2-token"

# Logging
LOG = logging.getLogger("xcube.clms")
LEVEL = logging.INFO
LOG.setLevel(LEVEL)
if not LOG.hasHandlers():
    handler = logging.StreamHandler()
    handler.setLevel(LEVEL)
    formatter = logging.Formatter(
        "%(name)s - %(asctime)s -  %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    LOG.addHandler(handler)

# DataOpener IDs
DATA_OPENER_IDS = (
    f"dataset:netcdf:{DATA_STORE_ID}",
    f"dataset:geotiff:{DATA_STORE_ID}",
)
