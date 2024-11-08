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
import time
from typing import Optional, Any, get_args, Container
from urllib.error import HTTPError
from urllib.parse import urlencode

import jwt
import requests
import xarray as xr
from requests import RequestException
from xcube.core.store import DataTypeLike

from xcube_clms.constants import (
    SEARCH_ENDPOINT,
    PORTAL_TYPE,
    HEADERS,
    LOG,
    CLMS_DATA_ID,
    METADATA_FIELDS,
    FULL_SCHEMA,
    DATASET_FORMAT,
    CLMS_API_AUTH,
)
from xcube_clms.utils import is_valid_data_type


class CLMS:

    def __init__(
        self,
        url: str,
        credentials: dict,
    ):
        self._url = url
        self._datasets_info: list[dict[str, Any]] = []
        self._metadata: list[str] = []
        self._api_token = CLMSAPIToken(credentials=credentials).access_token

    def open_dataset(self, data_id: str, **open_params) -> xr.Dataset:
        raise NotImplementedError

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

            response = self._make_api_request(self._build_api_url(SEARCH_ENDPOINT))

            while True:
                self._datasets_info.extend(response.get("items", []))
                next_page = response.get("batching", {}).get("next")
                if not next_page:
                    break
                response = self._make_api_request(next_page)

        return self._datasets_info

    def _get_metadata_fields(self):
        if not self._metadata:
            response = self._make_api_request(self._build_api_url(SEARCH_ENDPOINT))
            items = response.get("items", [])

            if len(items) > 0:
                self._metadata = list(items[0].keys())
        return self._metadata

    @staticmethod
    def _make_api_request(url: str, headers: dict = HEADERS, data: dict = None) -> dict:
        try:
            if data:
                response = requests.get(url, headers=headers, data=data)
            else:
                response = requests.get(url, headers=headers)

            response.raise_for_status()
            return response.json()
        except (HTTPError, RequestException, ValueError) as err:
            LOG.error(f"API error: {err}")
            return {}

    def _build_api_url(
        self, api_endpoint: str, metadata_fields: Optional[list] = None
    ) -> str:
        params = PORTAL_TYPE
        params[FULL_SCHEMA] = "1"
        if metadata_fields:
            params[METADATA_FIELDS] = ",".join(metadata_fields)

        query_params = urlencode(params)

        return f"{self._url}/{api_endpoint}/?{query_params}"

    @staticmethod
    def _convert_list_dict_to_list_str(data: list[dict[str, Any]]) -> list[str]:
        return [list(d.values())[0] for d in data]

    def access_item(self, data_id) -> dict:
        datasets = [
            item for item in self._datasets_info if data_id == item[CLMS_DATA_ID]
        ]
        if len(datasets) != 1:
            raise Exception(
                f"Expected one item for data_id: {data_id}, found {len(datasets)}."
            )
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

        assert (
            len(geographic_bounding_box) == 1
        ), f"Expected 1 bbox, got {len(geographic_bounding_box)}"
        assert len(crs) == 1, f"Expected 1 crs, got {len(crs)}"

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


class CLMSAPIToken:
    def __init__(
        self,
        credentials: dict,
    ):
        self._credentials = credentials
        self._grant = self._create_JWT_grant()
        self.access_token = self._request_access_token()

    def _create_JWT_grant(self):
        private_key = self._credentials["private_key"].encode("utf-8")

        claim_set = {
            "iss": self._credentials["client_id"],
            "sub": self._credentials["user_id"],
            "aud": self._credentials["token_uri"],
            "iat": int(time.time()),
            "exp": int(time.time() + (60 * 60)),
        }
        return jwt.encode(claim_set, private_key, algorithm="RS256")

    def _request_access_token(self) -> str:
        headers = HEADERS
        headers["Content-Type"] = "application/x-www-form-urlencoded"

        data = {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": self._grant,
        }
        response = self._make_api_request(CLMS_API_AUTH, headers=headers, data=data)

        return response["access_token"]
