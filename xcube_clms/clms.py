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
import inspect
import io
import time
from typing import Optional, Any, get_args, Container
from urllib.parse import urlencode

import fsspec
import rioxarray
import xarray as xr
from xcube.core.store import DataTypeLike
from xcube.util.jsonschema import JsonObjectSchema, JsonStringSchema

from .api_token import CLMSAPIToken
from .constants import (
    DOWNLOAD_ENDPOINT,
    SEARCH_ENDPOINT,
    PORTAL_TYPE,
    ACCEPT_HEADER,
    LOG,
    CLMS_DATA_ID_KEY,
    METADATA_FIELDS,
    FULL_SCHEMA,
    UID_KEY,
    DOWNLOADABLE_FILES_KEY,
    ITEMS_KEY,
    FILE_ID_KEY,
    CONTENT_TYPE_HEADER,
    TASK_STATUS_ENDPOINT,
    STATUS_PENDING,
    STATUS_COMPLETE,
    PENDING,
    COMPLETE,
    UNDEFINED,
    RESULTS,
    BOUNDING_BOX_KEY,
    CRS_KEY,
    START_TIME_KEY,
    END_TIME_KEY,
    DOWNLOAD_URL_KEY,
    STATUS_KEY,
    DATASETS_KEY,
    DATASET_ID_KEY,
    JSON_TYPE,
    BATCH,
    NEXT,
    BYTES_TYPE,
    FILENAME_KEY,
    NAME_KEY,
    NOT_SUPPORTED_LIST,
    DATASET_DOWNLOAD_INFORMATION_KEY,
    PATH_KEY,
    SOURCE_KEY,
    FULL_SOURCE_KEY,
    TITLE_KEY,
    SCHEMA_KEY,
    PROPERTIES_KEY,
    ALLOWED_SCHEMA_PARAMS,
    FILE_KEY,
)
from .utils import (
    is_valid_data_type,
    make_api_request,
    get_dataset_download_info,
    get_authorization_header,
    get_response_of_type,
)


class CLMS:

    # TODO: change get_data_id def and doc in the base class (ABC) to let the
    #  include_attrs kw arg be of type boolean (should be done in a diff PR)
    # TODO: preload_data should take in tuple of str
    #  It should return a preload handle. It can be used to cancel the preloading,
    #  - check the progress,
    #  - initiate the download,
    #  - it should have a progress bar for download. Its representation should be a progress bar in JN, (indeterminate Progress Bar)
    #  Create new store, init new file store ("filesystem")
    #  preload_data(..., non_blocking=True) => this should not go into ABC

    def __init__(
        self,
        url: str,
        credentials: dict = None,
    ):
        self._url = url
        self._datasets_info: list[dict[str, Any]] = []
        self._metadata: list[str] = []
        self._credentials: dict = {}
        self._data_ids = []
        if credentials:
            self._credentials = credentials
            self._clms_api_token_instance = CLMSAPIToken(credentials=credentials)
            self._api_token: str = self._clms_api_token_instance.access_token
        self.download_url: str = ""

    def set_credentials(self, credentials: dict):
        self._credentials = credentials
        self._clms_api_token_instance = CLMSAPIToken(credentials=credentials)
        self._api_token: str = self._clms_api_token_instance.access_token

    def refresh_token(self):
        if not self._api_token or self._clms_api_token_instance.is_token_expired():
            LOG.info("Token expired or not present. Refreshing token.")
            self._api_token = self._clms_api_token_instance.refresh_token()
        else:
            LOG.info("Current token valid. Reusing it.")

    def open_dataset(
        self,
        data_id: str,
        spatial_coverage: str = "",
        resolution: str = "",
        title: str = "",
        **open_params,
    ) -> xr.Dataset:
        if not self._credentials:
            raise Exception(
                "You need credentials to open the data. Please provide credentials using set_credentials()."
            )
        self.refresh_token()
        self._fetch_all_datasets()
        item = self.access_item(data_id)

        downloadable_item = item.get(DOWNLOADABLE_FILES_KEY).get(ITEMS_KEY)[0]
        path = ""
        source = ""
        full_source = ""
        if PATH_KEY in downloadable_item:
            path = downloadable_item.get(PATH_KEY, "")
        if SOURCE_KEY in downloadable_item:
            source = downloadable_item.get(SOURCE_KEY, "")
        if FULL_SOURCE_KEY in downloadable_item:
            full_source = (
                item.get(DATASET_DOWNLOAD_INFORMATION_KEY)
                .get(ITEMS_KEY)[0]
                .get(FULL_SOURCE_KEY, "")
            )

        assert (path is not "" and source is not "") or (
            full_source in NOT_SUPPORTED_LIST
        ), f"This data product: {item[TITLE_KEY]} is not yet supported in this plugin yet."

        request_exists: bool = False
        while True:
            status, task_id = self._current_requests(item[UID_KEY])
            if status == COMPLETE:
                LOG.info(
                    f"Download request with task id {task_id} already completed for data id: {data_id}"
                )
                request_exists = True
                break
            if status == UNDEFINED:
                LOG.info(f"No download request exists for data id: {data_id}")
                break
            if status == PENDING:
                LOG.info(
                    f"Download request with task id {task_id} already exists for data id: {data_id}. Status check again in 60 seconds"
                )
                time.sleep(60)

        if not request_exists:
            download_request_url, headers, json = self._prepare_download_request(
                item, data_id, spatial_coverage, resolution, title
            )

            response_data = make_api_request(
                method="POST", url=download_request_url, headers=headers, json=json
            )
            response = get_response_of_type(response_data, JSON_TYPE)
            LOG.info(f"Download Requested with Task ID : {response}")

            while True:
                status, task_id = self._current_requests(item[UID_KEY])
                if status == COMPLETE:
                    LOG.info(
                        f"Download request with task id {task_id} completed for data id: {data_id}"
                    )
                    break
                if status == PENDING:
                    LOG.info(
                        f"Download request with task id {task_id} for data id: {data_id}. Status check again in 60 seconds"
                    )
                    time.sleep(60)

        if self.download_url:
            LOG.info(f"Downloading zip file from {self.download_url}")
            headers = ACCEPT_HEADER.copy()
            headers.update(CONTENT_TYPE_HEADER)
            response_data = make_api_request(
                self.download_url, headers=headers, stream=True
            )
            response = get_response_of_type(response_data, BYTES_TYPE)

            with io.BytesIO(response.content) as zip_file_in_memory:
                fs = fsspec.filesystem("zip", fo=zip_file_in_memory)
                zip_contents = fs.ls(RESULTS)
                actual_zip_file = None
                if len(zip_contents) == 1:
                    if ".zip" in zip_contents[0][FILENAME_KEY]:
                        actual_zip_file = zip_contents[0]
                elif len(zip_contents) > 1:
                    LOG.warn("Cannot handle more than one zip files at the moment.")
                else:
                    LOG.info("No downloadable zip file found inside.")
                if actual_zip_file:
                    LOG.info(f"Found one zip file {actual_zip_file}.")
                    with fs.open(actual_zip_file[NAME_KEY], "rb") as f:
                        zip_fs = fsspec.filesystem("zip", fo=f)
                        geo_file = self._find_geo_in_dir(
                            "/",
                            zip_fs,
                        )
                        if geo_file:
                            with zip_fs.open(geo_file, "rb") as geo_f:
                                if "tif" in geo_file:
                                    return rioxarray.open_rasterio(geo_f)
                                else:
                                    return xr.open_dataset(geo_f)
                        else:
                            raise Exception("No GeoTiff file found")

        else:
            raise Exception(f"No DownloadURL found for data_id {data_id}")

    @staticmethod
    def _find_geo_in_dir(path, zip_fs):
        geo_file: str = ""
        contents = zip_fs.ls(path)
        for item in contents:
            if zip_fs.isdir(item[NAME_KEY]):
                geo_file = CLMS._find_geo_in_dir(
                    item[NAME_KEY],
                    zip_fs,
                )
                if geo_file:
                    return geo_file
            else:
                if item[NAME_KEY].endswith(".tif"):
                    LOG.info(f"Found TIFF file: {item[NAME_KEY]}")
                    geo_file = item["name"]
                    return geo_file
                if item[NAME_KEY].endswith(".nc"):
                    LOG.info(f"Found NetCDF file: {item[NAME_KEY]}")
                    geo_file = item[NAME_KEY]
                    return geo_file
        return geo_file

    def _prepare_download_request(
        self, item: dict, data_id: str, params: dict
    ) -> tuple[str, dict, dict]:
        LOG.info(f"Preparing download request for {data_id}")
        prepackaged_items = item[DOWNLOADABLE_FILES_KEY][ITEMS_KEY]
        if len(prepackaged_items) == 0:
            raise Exception(f"No prepackaged item found for {data_id}.")
        item_to_download = [
            item
            for item in prepackaged_items[DOWNLOADABLE_FILES_KEY][ITEMS_KEY]
            if all(item.get(key) == value for key, value in params.items())
        ]

        if len(item_to_download) == 0:
            raise Exception(
                f"No prepackaged item found for {data_id}. Please check preload parameters and/or data_id."
            )

        elif len(item_to_download) > 1:
            raise Exception(
                f"Multiple prepackaged items found for {data_id}. "
                f"Please specify the preload parameters to "
                f"to select one dataset for download"
            )

        dataset_id = self._filter_dataset_attrs([UID_KEY], [item])[0][UID_KEY]
        file_id = item_to_download[0][FILE_ID_KEY]
        json = get_dataset_download_info(
            dataset_id=dataset_id,
            file_id=file_id,
        )
        url = self._build_api_url(DOWNLOAD_ENDPOINT, datasets_request=False)
        if not self._api_token:
            self.refresh_token()
        headers = ACCEPT_HEADER.copy()
        headers.update(CONTENT_TYPE_HEADER)
        headers.update(get_authorization_header(self._api_token))

        return url, headers, json

    def _filter_dataset_attrs(
        self, attrs: Container[str], raw_datasets: list[dict[str, Any]] = None
    ) -> list[dict[str, Any]]:
        """
        Filters attributes of datasets in a list

        Args:
            attrs: A container of attribute names to filter for in each dataset.
            raw_datasets: List of datasets metadata where each dataset contains the same keys

        Returns:
           A list of dictionaries with filtered keys.
        """
        if raw_datasets:
            supported_keys = list(raw_datasets[0].keys())
        else:
            supported_keys = self._attrs

        raw_datasets = raw_datasets or self._fetch_all_datasets()

        signature = inspect.signature(self._filter_dataset_attrs)
        if (attrs is not None and not isinstance(attrs, (list, set, tuple))) | (
            attrs is None
        ):
            raise TypeError(
                f"Expected instance "
                f"{get_args(signature.parameters.get('attrs').annotation)}, "
                f"got {type(attrs).__name__}"
            )

        return [
            {key: item[key] for key in supported_keys if key in attrs}
            for item in raw_datasets
        ]

    def _fetch_all_datasets(self) -> list[dict[str, Any]]:
        if not self._datasets_info:
            LOG.info(
                f"Datasets not fetched yet. Fetching all datasets now from {self._url}"
            )

            response_data = make_api_request(self._build_api_url(SEARCH_ENDPOINT))
            while True:
                response = get_response_of_type(response_data, JSON_TYPE)
                self._datasets_info.extend(response.get(ITEMS_KEY, []))
                next_page = response.get(BATCH, {}).get(NEXT)
                if not next_page:
                    break
                response_data = make_api_request(next_page)
            self._attrs = list(self._datasets_info[0].keys())
        return self._datasets_info

    def _build_api_url(
        self,
        api_endpoint: str,
        metadata_fields: Optional[list] = None,
        datasets_request: bool = True,
    ) -> str:
        params = {}
        if datasets_request:
            params = PORTAL_TYPE
            params[FULL_SCHEMA] = "1"
        if metadata_fields:
            params[METADATA_FIELDS] = ",".join(metadata_fields)
        if params:
            query_params = urlencode(params)
            return f"{self._url}/{api_endpoint}/?{query_params}"
        return f"{self._url}/{api_endpoint}"

    def access_item(self, data_id) -> dict:
        products = self._filter_dataset_attrs(
            [CLMS_DATA_ID_KEY, DOWNLOADABLE_FILES_KEY]
        )
        clms_data_product_id, dataset_filename = data_id.split(":")
        dataset = []
        for product in products:
            for item in product.get(DOWNLOADABLE_FILES_KEY).get(ITEMS_KEY):
                if (
                    item.get("file") == dataset_filename
                    and product.get(CLMS_DATA_ID_KEY) == clms_data_product_id
                ):
                    dataset.append(item)

        if len(dataset) > 1:
            raise Exception(
                f"Expected one item for data_id: {data_id}, found {len(dataset)}."
            )
        if len(dataset) == 0:
            raise Exception(f"Data id: {data_id} not found in the CLMS catalog")
        return dataset[0]

    def get_data_ids(self) -> list[str]:
        return self._create_data_ids()

    def _create_data_ids(self) -> list[str]:
        if not self._datasets_info:
            self._fetch_all_datasets()
        if self._data_ids:
            return self._data_ids

        self._data_ids = []
        for item in self._datasets_info:
            for i in item[DOWNLOADABLE_FILES_KEY][ITEMS_KEY]:
                if "file" in i and i[FILE_KEY] != "":
                    self._data_ids.append(f"{item[CLMS_DATA_ID_KEY]}:{i[FILE_KEY]}")

        return self._data_ids

    def get_data_ids_with_attrs(
        self, attrs: Container[str], data_id: str
    ) -> tuple[str, dict[str, Any]]:
        """Extracts the desired attributes based on the data_id from the list of datasets."""
        self._fetch_all_datasets()
        item = self.access_item(data_id)
        dataset = self._filter_dataset_attrs(attrs, [item])
        return data_id, dataset[0]

    def has_data(self, data_id: str, data_type: DataTypeLike = None) -> bool:
        if is_valid_data_type(data_type):
            self._fetch_all_datasets()
            datasets = [
                item
                for item in self._datasets_info
                if data_id == item[CLMS_DATA_ID_KEY]
            ]
            if len(datasets) == 0:
                return False
            return True
        return False

    def get_extent(self, data_id: str) -> dict:
        self._fetch_all_datasets()
        item = self.access_item(data_id)
        geographic_bounding_box = item.get(BOUNDING_BOX_KEY).get(ITEMS_KEY)
        crs = item.get(CRS_KEY)
        time_range = (item.get(START_TIME_KEY), item.get(END_TIME_KEY))

        if len(geographic_bounding_box) > 1:
            LOG.warning(
                f"Expected 1 bbox, got {len(geographic_bounding_box)}. Outputting the first element."
            )
        if len(crs) > 1:
            LOG.warning(
                f"Expected 1 crs, got {len(crs)}. Outputting the first element."
            )

        # TODO: Handle multiple bounding boxes in the same item
        bbox = [
            float(geographic_bounding_box[0]["west"]),  # x1
            float(geographic_bounding_box[0]["south"]),  # y1
            float(geographic_bounding_box[0]["east"]),  # x2
            float(geographic_bounding_box[0]["north"]),  # y2
        ]

        return dict(bbox=bbox, time_range=time_range, crs=crs[0])

    def get_preload_params(self, data_id: str) -> dict[str : str | None]:
        self._fetch_all_datasets()
        preload_params = [
            data[DOWNLOADABLE_FILES_KEY][ITEMS_KEY]
            for data in self._datasets_info
            if data[CLMS_DATA_ID_KEY] == data_id
        ][0]

        return preload_params

    def get_preload_data_params_schema_for_data(self, data_id: str):
        self._fetch_all_datasets()
        raw_schema = [
            data[DOWNLOADABLE_FILES_KEY][SCHEMA_KEY][PROPERTIES_KEY]
            for data in self._datasets_info
            if data[CLMS_DATA_ID_KEY] == data_id
        ][0]
        params = {}
        for item, inner_dict in raw_schema.items():
            param_data = {}
            for inner_item, val in inner_dict.items():
                if any(key in inner_item for key in ALLOWED_SCHEMA_PARAMS):
                    param_data[inner_item] = val
            params[item] = JsonStringSchema(**param_data)
        schema = JsonObjectSchema(properties=params)
        return schema

    def _current_requests(self, dataset_id: str) -> tuple:
        self.refresh_token()
        headers = ACCEPT_HEADER.copy()
        headers.update(get_authorization_header(self._api_token))
        url = self._build_api_url(TASK_STATUS_ENDPOINT, datasets_request=False)
        response_data = make_api_request(url=url, headers=headers)
        response = get_response_of_type(response_data, JSON_TYPE)
        for key in response:
            status = response[key][STATUS_KEY]
            datasets = response[key][DATASETS_KEY]
            requested_data_id = ""
            if isinstance(datasets, list):
                requested_data_id = datasets[0][DATASET_ID_KEY]
            elif isinstance(datasets, dict):
                requested_data_id = datasets[DATASET_ID_KEY]
            else:
                LOG.warn(f"No DatasetID found in response {datasets}")
            if status in STATUS_PENDING and dataset_id == requested_data_id:
                return PENDING, key
            if status in STATUS_COMPLETE and dataset_id == requested_data_id:
                if isinstance(response[key], list):
                    self.download_url = response[key][0][DOWNLOAD_URL_KEY]
                elif isinstance(response[key], dict):
                    self.download_url = response[key][DOWNLOAD_URL_KEY]
                return COMPLETE, key

        return UNDEFINED, ""

    def _queue_download(self, data_request: dict) -> str:
        if not self._credentials:
            raise Exception(
                "You need credentials to open the data. Please provide credentials using set_credentials()."
            )
        self.refresh_token()
        self._fetch_all_datasets()
        data_id = data_request.get("data_id", "")
        preload_params = data_request.get("preload_params", {})

        item = self.access_item(data_id)

        # This is to make sure that there are pre-packaged files available for download.
        # Without this, the API throws the following error: Error, the FileID is not valid.
        # We check for path and source based on the API code here:
        # https://github.com/eea/clms.downloadtool/blob/master/clms/downloadtool/api/services/datarequest_post/post.py#L177-L196

        path = item.get(DOWNLOADABLE_FILES_KEY).get(ITEMS_KEY).get("path", "")
        source = item.get(DOWNLOADABLE_FILES_KEY).get(ITEMS_KEY).get("source", "")

        assert (
            path is not "" and source is not ""
        ), f"This data product: {item["title"]} is not yet supported in this plugin yet."

        request_exists: bool = False
        while True:
            status, task_id = self._current_requests(item[UID_KEY])
            if status == COMPLETE:
                LOG.info(
                    f"Download request with task id {task_id} already completed for data id: {data_id}"
                )
                request_exists = True
                break
            if status == UNDEFINED:
                LOG.info(f"No download request exists for data id: {data_id}")
                break
            if status == PENDING:
                LOG.info(
                    f"Download request with task id {task_id} already exists for data id: {data_id}. Status check again in 60 seconds"
                )
                time.sleep(60)

        if not request_exists:
            download_request_url, headers, json = self._prepare_download_request(
                item, data_id, preload_params
            )

            response_data = make_api_request(
                method="POST", url=download_request_url, headers=headers, json=json
            )
            response = get_response_of_type(response_data, JSON_TYPE)
            LOG.info(f"Download Requested with Task ID : {response}")

            while True:
                status, task_id = self._current_requests(item[UID_KEY])
                if status == COMPLETE:
                    LOG.info(
                        f"Download request with task id {task_id} completed for data id: {data_id}"
                    )
                    break
                if status == PENDING:
                    LOG.info(
                        f"Download request with task id {task_id} for data id: {data_id}. Status check again in 60 seconds"
                    )
                    time.sleep(60)

        if self.download_url:
            LOG.info(f"Downloading zip file from {self.download_url}")
            headers = ACCEPT_HEADER.copy()
            headers.update(CONTENT_TYPE_HEADER)
            response_data = make_api_request(
                self.download_url, headers=headers, stream=True
            )
            response = get_response_of_type(response_data, BYTES_TYPE)

            with io.BytesIO(response.content) as zip_file_in_memory:
                fs = fsspec.filesystem("zip", fo=zip_file_in_memory)
                zip_contents = fs.ls(RESULTS)
                actual_zip_file = None
                if len(zip_contents) == 1:
                    if ".zip" in zip_contents[0][FILENAME_KEY]:
                        actual_zip_file = zip_contents[0]
                elif len(zip_contents) > 1:
                    LOG.warn("Cannot handle more than one zip files at the moment.")
                else:
                    LOG.info("No downloadable zip file found inside.")
                if actual_zip_file:
                    LOG.info(f"Found one zip file {actual_zip_file}.")
                    with fs.open(actual_zip_file[NAME_KEY], "rb") as f:
                        zip_fs = fsspec.filesystem("zip", fo=f)
                        geo_file = self._find_geo_in_dir(
                            "/",
                            zip_fs,
                        )
                        if geo_file:
                            with zip_fs.open(geo_file, "rb") as geo_f:
                                if "tif" in geo_file:
                                    return rioxarray.open_rasterio(geo_f)
                                else:
                                    return xr.open_dataset(geo_f)
                        else:
                            raise Exception("No GeoTiff file found")

        else:
            raise Exception(f"No DownloadURL found for data_id {data_id}")

    def preload_data(self, data_requests: list[dict]):
        task_ids = []
        for data_request in data_requests:
            task_id = self._queue_download(data_request)
            task_ids.append(task_id)
