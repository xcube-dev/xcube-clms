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

# Configuration constants
TIME_TO_EXPIRE = 48  # (in hours)
RETRY_TIMEOUT = 60  # (in seconds)

DATA_STORE_ID = "clms"

# CLMS API Constants
SEARCH_ENDPOINT = "@search"
DOWNLOAD_ENDPOINT = "@datarequest_post"
TASK_STATUS_ENDPOINT = "@datarequest_search"
CANCEL_ENDPOINT = "@datarequest_delete"

BATCH = "batching"
NEXT = "next"

PORTAL_TYPE = {"portal_type": "DataSet"}
ACCEPT_HEADER = {"Accept": "application/json"}
CONTENT_TYPE_HEADER = {"Content-Type": "application/json"}

METADATA_FIELDS = "metadata_fields"
FULL_SCHEMA = "fullobjects"

DATA_ID_SEPARATOR = "|"

# Dict keys

DATA_ID_KEY = "data_id"
CLMS_DATA_ID_KEY = "id"
DATASET_FORMAT_KEY = "distribution_format_list"
UID_KEY = "UID"
DOWNLOADABLE_FILES_KEY = "downloadable_files"
DATASET_DOWNLOAD_INFORMATION_KEY = "dataset_download_information"
ITEMS_KEY = "items"
SPATIAL_COVERAGE_KEY = "area"
RESOLUTION_KEY = "resolution"
FORMAT_KEY = "format"
ID_KEY = "@id"
FILE_ID_KEY = "FileID"
FILE_KEY = "file"
BOUNDING_BOX_KEY = "geographicBoundingBox"
CRS_KEY = "coordinateReferenceSystemList"
START_TIME_KEY = "temporalExtentStart"
END_TIME_KEY = "temporalExtentEnd"
DOWNLOAD_URL_KEY = "DownloadURL"
STATUS_KEY = "Status"
DATASETS_KEY = "Datasets"
DATASET_ID_KEY = "DatasetID"
FILENAME_KEY = "filename"
NAME_KEY = "name"
TITLE_KEY = "title"
SCHEMA_KEY = "schema"
PROPERTIES_KEY = "properties"
PATH_KEY = "path"
SOURCE_KEY = "source"
FULL_SOURCE_KEY = "full_source"
DESCRIPTION_KEY = "description"
ENUM_KEY = "enum"
TASK_IDS_KEY = "TaskIds"
TASK_ID_KEY = "TaskID"
DOWNLOAD_AVAILABLE_TIME_KEY = "FinalizationDateTime"
ORIGINAL_FILENAME_KEY = "orig_filename"
ITEM_KEY = "item"
PRODUCT_KEY = "product"

STATUS_PENDING = ["Queued", "In_progress"]
STATUS_COMPLETE = "Finished_ok"
STATUS_CANCELLED = "Cancelled"
STATUS_REJECTED = "Rejected"

PENDING = "PENDING"
COMPLETE = "COMPLETE"
UNDEFINED = "UNDEFINED"
EXPIRED = "EXPIRED"
REJECTED = "REJECTED"
RESULTS = "Results/"

NOT_SUPPORTED_LIST = ["WEKEO", "LEGACY", "LANDCOVER"]
ALLOWED_SCHEMA_PARAMS = [TITLE_KEY, DESCRIPTION_KEY, ENUM_KEY]

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

JSON_TYPE = "json"
BYTES_TYPE = "bytes"

# Progress weights (should sum to 1)
QUEUE_WEIGHT = 0.9
DOWNLOAD_WEIGHT = 0.5
EXTRACTION_WEIGHT = 0.5

# Three State event const
NOT_STARTED = "NOT_STARTED"
STARTED = "STARTED"
FINISHED = "FINISHED"
