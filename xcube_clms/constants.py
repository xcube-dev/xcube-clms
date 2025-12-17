# The MIT License (MIT)
# Copyright (c) 2025 by the xcube development team and contributors
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

CLMS_API_URL = "https://land.copernicus.eu/api"

# Configuration constants
TIME_TO_EXPIRE = 24  # (in hours)
RETRY_TIMEOUT = 15  # (in seconds)

# Folders
DEFAULT_PRELOAD_CACHE_FOLDER = "clms_cache/"
DOWNLOAD_FOLDER = "downloads"

# CLMS API Constants
SEARCH_ENDPOINT = "@search"
DOWNLOAD_ENDPOINT = "@datarequest_post"
TASK_STATUS_ENDPOINT = "@datarequest_search"
CANCEL_ENDPOINT = "@datarequest_delete"
GET_DOWNLOAD_FILE_URLS_ENDPOINT = "@get-download-file-urls"

# List of dataset sources that are available via CLMS.
# "LEGACY"
# "LANDCOVER"
# "HOTSPOTS"
# "VITO_Geotiff_LSP"
# "WEKEO"
# "EEA"

EEA = "eea"
CDSE = "cdse"
SUPPORTED_DATASET_SOURCES = [EEA, CDSE]

# Headers
ACCEPT_HEADER = {"Accept": "application/json"}
CONTENT_TYPE_HEADER = {"Content-Type": "application/json"}

# Separator for creating data id
DATA_ID_SEPARATOR = "|"

# CLMS METADATA KEYS
ID_KEY = "@id"
UID_KEY = "UID"
ITEM_KEY = "item"
NAME = "name"
ITEMS_KEY = "items"
FILE_KEY = "file"
FORMAT_KEY = "format"
PRODUCT_KEY = "product"
FULL_SOURCE = "full_source"
CLMS_DATA_ID_KEY = "id"
CRS_KEY = "coordinateReferenceSystemList"
DOWNLOADABLE_FILES_KEY = "downloadable_files"
DATASET_DOWNLOAD_INFORMATION = "dataset_download_information"

# Request status
PENDING = "PENDING"
COMPLETE = "COMPLETE"
CANCELLED = "CANCELLED"

# Logging
LOG = logging.getLogger("xcube.clms")
