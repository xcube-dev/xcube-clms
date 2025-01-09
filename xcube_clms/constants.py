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

CLMS_API_URL = "https://land.copernicus.eu/api"

# Configuration constants
TIME_TO_EXPIRE = 24  # (in hours)
RETRY_TIMEOUT = 60  # (in seconds)

DEFAULT_PRELOAD_CACHE_FOLDER = "clms_cache/"

# CLMS API Constants
SEARCH_ENDPOINT = "@search"
DOWNLOAD_ENDPOINT = "@datarequest_post"
TASK_STATUS_ENDPOINT = "@datarequest_search"
CANCEL_ENDPOINT = "@datarequest_delete"

# Headers
ACCEPT_HEADER = {"Accept": "application/json"}
CONTENT_TYPE_HEADER = {"Content-Type": "application/json"}

# Separator for creating data id
DATA_ID_SEPARATOR = "|"

# Request status
PENDING = "PENDING"
COMPLETE = "COMPLETE"
CANCELLED = "CANCELLED"

ITEM_KEY = "item"
PRODUCT_KEY = "product"

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
    f"dataset:geotiff:{DATA_STORE_ID}",
    f"dataset:zarr:{DATA_STORE_ID}",
)
