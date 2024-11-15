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
import jwt
import requests
import rioxarray
import xarray as xr
from xcube.core.store import DataTypeLike

from .constants import (
    DOWNLOAD_ENDPOINT,
    SEARCH_ENDPOINT,
    PORTAL_TYPE,
    ACCEPT_HEADER,
    LOG,
    CLMS_DATA_ID,
    METADATA_FIELDS,
    FULL_SCHEMA,
    DATASET_FORMAT,
    CLMS_API_AUTH,
    UID,
    DOWNLOADABLE_FILES,
    ITEMS,
    SPATIAL_COVERAGE,
    RESOLUTION,
    FILE_ID,
    CONTENT_TYPE_HEADER,
    TASK_STATUS_ENDPOINT,
    STATUS_PENDING,
    STATUS_COMPLETE,
    PENDING,
    COMPLETE,
    UNDEFINED,
    RESULTS,
)
from .utils import (
    is_valid_data_type,
    make_api_request,
    get_dataset_download_info,
    get_authorization_header,
    get_response_of_type,
)


class CLMS:

    def __init__(
        self,
        url: str,
        credentials: dict = None,
    ):
        self._url = url
        self._datasets_info: list[dict[str, Any]] = []
        self._metadata: list[str] = []
        self._credentials: dict = {}
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
        **open_params,
    ) -> xr.Dataset:
        if not self._credentials:
            raise Exception(
                "You need credentials to open the data. Please provide credentials using set_credentials()."
            )
        self.refresh_token()
        self._fetch_all_datasets()
        item = self.access_item(data_id)

        request_exists: bool = False
        while True:
            status, task_id = self._current_requests(item[UID])
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
                item, data_id, spatial_coverage, resolution
            )

            response_data = make_api_request(
                method="POST", url=download_request_url, headers=headers, json=json
            )
            response = get_response_of_type(response_data, "json")
            LOG.info(f"Download Requested with Task ID : {response}")

            while True:
                status, task_id = self._current_requests(item[UID])
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
            response = get_response_of_type(response_data, "bytes")

            with io.BytesIO(response.content) as zip_file_in_memory:
                fs = fsspec.filesystem("zip", fo=zip_file_in_memory)
                zip_contents = fs.ls(RESULTS)
                actual_zip_file = None
                if len(zip_contents) == 1:
                    if ".zip" in zip_contents[0]["filename"]:
                        actual_zip_file = zip_contents[0]
                elif len(zip_contents) > 1:
                    LOG.warn("Cannot handle more than one zip files at the moment.")
                else:
                    LOG.info("No downloadable zip file found inside.")
                if actual_zip_file:
                    LOG.info(f"Found one zip file {actual_zip_file}.")
                    with fs.open(actual_zip_file["name"], "rb") as f:
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
            if zip_fs.isdir(item["name"]):
                geo_file = CLMS._find_geo_in_dir(
                    item["name"],
                    zip_fs,
                )
                if geo_file:
                    return geo_file
            else:
                if item["name"].endswith(".tif"):
                    LOG.info(f"Found TIFF file: {item["name"]}")
                    geo_file = item["name"]
                    return geo_file
                if item["name"].endswith(".nc"):
                    LOG.info(f"Found NetCDF file: {item["name"]}")
                    geo_file = item["name"]
                    return geo_file
        return geo_file

    def _prepare_download_request(
        self,
        item: dict,
        data_id: str,
        spatial_coverage: str = "",
        resolution: str = "",
    ) -> tuple[str, dict, dict]:
        LOG.info(f"Preparing download request for {data_id}")
        prepackaged_items = item[DOWNLOADABLE_FILES][ITEMS]
        if len(prepackaged_items) == 0:
            raise Exception(f"No prepackaged item found for {data_id}.")
        item_to_download = [
            item
            for item in prepackaged_items
            if (
                (item[SPATIAL_COVERAGE] == spatial_coverage)
                & (item[RESOLUTION] == resolution)
            )
        ]

        if len(item_to_download) == 0:
            raise Exception(
                f"No prepackaged item found for {data_id}. Please check resolution and spatial coverage."
            )

        elif len(item_to_download) > 1:
            raise Exception(
                f"Multiple prepackaged items found for {data_id}. "
                f"Please specify the resolution and spatial coverage "
                f"to select one dataset for download"
            )

        dataset_id = self._filter_dataset_attrs([UID], [item])[0][UID]
        file_id = item_to_download[0][FILE_ID]
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
        self, attrs: Container[str], datasets: list[dict[str, Any]] = None
    ) -> list[dict[str, Any]]:
        """
        Filter datasets based on specified attributes.

        Args:
            attrs: A container of attribute names to filter for in each dataset.

        Returns:
           A list of dictionaries with filtered datasets.
        """
        datasets = datasets or self._fetch_all_datasets()

        supported_keys = self._get_metadata_fields()

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
            for item in datasets
        ]

    def _fetch_all_datasets(self) -> list[dict[str, Any]]:
        if not self._datasets_info:
            LOG.info(
                f"Datasets not fetched yet. Fetching all datasets now from {self._url}"
            )

            response_data = make_api_request(self._build_api_url(SEARCH_ENDPOINT))
            while True:
                response = get_response_of_type(response_data, "json")
                self._datasets_info.extend(response.get("items", []))
                next_page = response.get("batching", {}).get("next")
                if not next_page:
                    break
                response_data = make_api_request(next_page)

        return self._datasets_info

    def _get_metadata_fields(self):
        if not self._metadata:
            response_data = make_api_request(self._build_api_url(SEARCH_ENDPOINT))
            response = get_response_of_type(response_data, "json")
            items = response.get("items", [])
            if len(items) > 0:
                self._metadata = list(items[0].keys())

        return self._metadata

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

    # TODO: Move this out to utils
    @staticmethod
    def _convert_list_dict_to_list_str(data: list[dict[str, Any]]) -> list[str]:
        return [list(d.values())[0] for d in data]

    def access_item(self, data_id) -> dict:
        datasets = [
            item for item in self._datasets_info if data_id == item[CLMS_DATA_ID]
        ]
        if len(datasets) > 1:
            raise Exception(
                f"Expected one item for data_id: {data_id}, found {len(datasets)}."
            )
        if len(datasets) == 0:
            raise Exception(f"Data id: {data_id} not found in the CLMS catalog")
        return datasets[0]

    def get_data_ids(self) -> list[str]:
        self._fetch_all_datasets()
        data_ids_with_keys = self._filter_dataset_attrs([CLMS_DATA_ID])
        return self._convert_list_dict_to_list_str(data_ids_with_keys)

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
                item for item in self._datasets_info if data_id == item[CLMS_DATA_ID]
            ]
            if len(datasets) == 0:
                return False
            return True
        return False

    def get_extent(self, data_id: str) -> dict:
        self._fetch_all_datasets()
        item = self.access_item(data_id)
        geographic_bounding_box = item.get("geographicBoundingBox").get("items")
        crs = item.get("coordinateReferenceSystemList")
        time_range = (item.get("temporalExtentStart"), item.get("temporalExtentEnd"))

        if len(geographic_bounding_box) == 1:
            LOG.warning(f"Expected 1 bbox, got {len(geographic_bounding_box)}. Outputting the first element.")
        if len(crs) == 1:
            LOG.warning(f"Expected 1 crs, got {len(crs)}. Outputting the first element.")

        # TODO: Handle multiple bounding boxes in the same item
        bbox = [
            float(geographic_bounding_box[0]["west"]),  # x1
            float(geographic_bounding_box[0]["south"]),  # y1
            float(geographic_bounding_box[0]["east"]),  # x2
            float(geographic_bounding_box[0]["north"]),  # y2
        ]

        return dict(bbox=bbox, time_range=time_range, crs=crs[0])

    def get_data_id_format(self, data_id: str) -> str:
        self._fetch_all_datasets()
        item = self.access_item(data_id)
        format_list = self._filter_dataset_attrs([DATASET_FORMAT], [item])
        return format_list[0].get(DATASET_FORMAT)[0]

    def get_spatial_coverage_and_resolution(
        self, data_id: str
    ) -> dict[str : str | None]:
        self._fetch_all_datasets()
        download_info = [
            data[DOWNLOADABLE_FILES][ITEMS]
            for data in self._datasets_info
            if data["id"] == data_id
        ]
        spatial_cov_res_list = []
        for info in download_info[0]:
            spatial_cov_res_list.append(
                {SPATIAL_COVERAGE: info[SPATIAL_COVERAGE], RESOLUTION: info[RESOLUTION]}
            )
        return spatial_cov_res_list

    def _current_requests(self, dataset_id: str) -> tuple:
        self.refresh_token()
        headers = ACCEPT_HEADER.copy()
        headers.update(get_authorization_header(self._api_token))
        url = self._build_api_url(TASK_STATUS_ENDPOINT, datasets_request=False)
        response_data = make_api_request(url=url, headers=headers)
        response = get_response_of_type(response_data, "json")
        for key in response:
            status = response[key]["Status"]
            datasets = response[key]["Datasets"]
            requested_data_id = ""
            if isinstance(datasets, list):
                requested_data_id = datasets[0]["DatasetID"]
            elif isinstance(datasets, dict):
                requested_data_id = datasets["DatasetID"]
            else:
                LOG.warn(f"No DatasetID found in response {datasets}")
            if status in STATUS_PENDING and dataset_id == requested_data_id:
                return PENDING, key
            if status in STATUS_COMPLETE and dataset_id == requested_data_id:
                if isinstance(response[key], list):
                    self.download_url = response[key][0]["DownloadURL"]
                elif isinstance(response[key], dict):
                    self.download_url = response[key]["DownloadURL"]
                return COMPLETE, key

        return UNDEFINED, ""


class CLMSAPIToken:
    def __init__(
        self,
        credentials: dict,
    ):
        self._credentials: dict = credentials
        self._token_expiry: int = 0
        self._token_lifetime: int = 3600  # Token lifetime in seconds
        self._expiry_margin: int = 300  # Refresh 5 minutes before expiration
        self._grant: str = self._create_JWT_grant()
        self.access_token: str = ""
        self.refresh_token()

    def _create_JWT_grant(self):
        private_key = self._credentials["private_key"].encode("utf-8")

        claim_set = {
            "iss": self._credentials["client_id"],
            "sub": self._credentials["user_id"],
            "aud": self._credentials["token_uri"],
            "iat": int(time.time()),
            "exp": int(time.time() + self._token_lifetime),
        }
        return jwt.encode(claim_set, private_key, algorithm="RS256")

    def _request_access_token(self) -> str:
        headers = ACCEPT_HEADER.copy()
        headers["Content-Type"] = "application/x-www-form-urlencoded"

        data = {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": self._grant,
        }
        response_data = make_api_request(
            CLMS_API_AUTH, headers=headers, data=data, method="POST"
        )
        response = get_response_of_type(response_data, "json")

        return response["access_token"]

    def is_token_expired(self) -> bool:
        return time.time() > (self._token_expiry - self._expiry_margin)

    def refresh_token(self) -> str:
        try:
            self.access_token = self._request_access_token()
            self._token_expiry = time.time() + self._token_lifetime
            LOG.info("Token refreshed successfully.")
        except requests.exceptions.RequestException as e:
            LOG.info("Token refresh failed:", e)
            raise e

        return self.access_token
