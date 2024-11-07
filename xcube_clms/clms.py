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
from typing import Optional, Any, get_args, Container
from urllib.error import HTTPError
from urllib.parse import urlencode

import requests
import xarray as xr
from requests import RequestException

from xcube_clms.constants import (
    SEARCH_ENDPOINT,
    PORTAL_TYPE,
    HEADERS,
    LOG,
    CLMS_DATA_ID,
    METADATA_FIELDS,
    FULL_SCHEMA,
)


class CLMS:

    def __init__(self, url: str):
        self._url = url
        self._datasets_info: list[dict[str, Any]] = []
        self._attrs: list[str] = []
        self._metadata: list[str] = []

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
            LOG.info(f"Fetching all datasets from {self._url}")

            api_url = self._build_api_url(SEARCH_ENDPOINT)
            response = self._make_api_request(api_url)

            while True:
                self._datasets_info.extend(response.get("items", []))
                next_page = response.get("batching", {}).get("next")
                if not next_page:
                    break
                response = self._make_api_request(next_page)

        return self._datasets_info

    def _get_metadata_fields(self):
        if not self._metadata:
            api_url = self._build_api_url(SEARCH_ENDPOINT)
            response = self._make_api_request(api_url)
            items = response.get("items", [])

            if len(items) > 0:
                self._metadata = list(items[0])
        return self._metadata

    @staticmethod
    def _make_api_request(url: str) -> dict:
        try:
            response = requests.get(url, headers=HEADERS)
            response.raise_for_status()
            return response.json()
        except (HTTPError, RequestException, ValueError) as err:
            LOG.error(f"API error: {err}")
            return {}

    def _build_api_url(
        self, api_endpoint: str, metadata_fields: Optional[list] = None
    ) -> str:
        params = PORTAL_TYPE
        if metadata_fields:
            params[METADATA_FIELDS] = ",".join(metadata_fields)
        else:
            params[FULL_SCHEMA] = "1"

        query_params = urlencode(params)

        return f"{self._url}/{api_endpoint}/?{query_params}"

    @staticmethod
    def _convert_list_dict_to_list_str(data: list[dict[str, Any]]) -> list[str]:
        return [list(d.values())[0] for d in data]

    def get_data_ids(self) -> list[str]:
        if self._datasets_info:
            self._fetch_all_datasets()
        data_ids_with_keys = self._filter_dataset_attrs([CLMS_DATA_ID])
        return self._convert_list_dict_to_list_str(data_ids_with_keys)

    def get_data_ids_with_attrs(
        self, attrs: Container[str], data_id: str
    ) -> tuple[str, dict[str, Any]]:
        """Extracts the desired attributes based on the data_id from the list of datasets."""
        if self._datasets_info:
            self._fetch_all_datasets()
        datasets = [
            item for item in self._datasets_info if data_id == item[CLMS_DATA_ID]
        ]
        dataset = self._filter_dataset_attrs(attrs, datasets)
        if len(dataset) > 1:
            raise Exception(
                f"More than one item found for data_id: {data_id} provided."
            )
        return data_id, dataset[0]
