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
from typing import Optional, List, Dict
from urllib.error import HTTPError
from urllib.parse import urlencode

import requests
import xarray as xr
from requests import RequestException

from xcube_clms.constants import SEARCH_ENDPOINT, PORTAL_TYPE, HEADERS


class CLMS:

    def __init__(self, url: str):
        self.url = url
        self.datasets_info: List[Dict] = []
        self.metadata_fields: List[str] = []
        self.filtered_dataset_info: List[Dict] = []

    def open_dataset(self, data_id: str, **open_params) -> xr.Dataset:
        raise NotImplementedError

    def search_datasets(self, metadata_fields: List[str]) -> List[Dict]:
        if (len(self.datasets_info) == 0) | (self.metadata_fields != metadata_fields):
            self.datasets_info = self._get_all_datasets(metadata_fields)

            self.filtered_dataset_info = self._filter_dataset_metadata_fields()

        return self.filtered_dataset_info

    def _filter_dataset_metadata_fields(self) -> List[Dict]:
        if self.metadata_fields:
            return [
                {key: item[key] for key in self.metadata_fields if key in item}
                for item in self.datasets_info
            ]
        else:
            return self.datasets_info

    def _get_all_datasets(self, metadata_fields: List[str]) -> List[Dict]:
        self.datasets_info = []
        self.metadata_fields = metadata_fields

        api_url = self.build_api_url(SEARCH_ENDPOINT, self.metadata_fields)
        b_start = 0
        b_size = 25

        while True:
            response = self._make_api_request(
                f"{api_url}&b_start={b_start}&b_size={b_size}"
            )
            items = response.get("items", [])
            self.datasets_info.extend(items)

            if len(items) < b_size:
                break

            b_start += b_size

        return self.datasets_info

    def get_api_metadata(self):
        api_url = self.build_api_url(SEARCH_ENDPOINT)
        response = self._make_api_request(api_url)
        items = response.get("items", [])

        if len(items) > 0:
            return list(items[0])
        return []

    @staticmethod
    def _make_api_request(url: str) -> dict:
        try:
            response = requests.get(url, headers=HEADERS)
            response.raise_for_status()
            return response.json()
        except (HTTPError, RequestException, ValueError) as err:
            print(f"API error: {err}")
            return {}

    def build_api_url(
        self, api_endpoint: str, metadata_fields: Optional[list] = None
    ) -> str:
        params = {"portal_type": PORTAL_TYPE}
        if metadata_fields:
            params["metadata_fields"] = ",".join(metadata_fields)
        else:
            params["fullobjects"] = "1"

        query_params = urlencode(params)

        return f"{self.url}/{api_endpoint}/?portal_type={PORTAL_TYPE}&{query_params}"
