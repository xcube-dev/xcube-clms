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
from typing import Optional, List, Dict, Union, Iterator, Any, get_args
from urllib.error import HTTPError
from urllib.parse import urlencode

import requests
import xarray as xr
from requests import RequestException

from xcube_clms.constants import SEARCH_ENDPOINT, PORTAL_TYPE, HEADERS, LOG


class CLMS:

    def __init__(self, url: str):
        self.url = url
        self._datasets_info: List[Dict] = []
        self._metadata_fields: List[str] = []
        self._filtered_dataset_info: List[Dict] = []
        self._metadata: List[str] = []

    def open_dataset(self, data_id: str, **open_params) -> xr.Dataset:
        raise NotImplementedError

    def search_datasets(
        self, metadata_fields: Optional[List[str] | None] = None
    ) -> List[Dict]:
        if len(self._datasets_info) == 0:
            self._datasets_info = self._get_all_datasets()

        signature = inspect.signature(self.search_datasets)
        if metadata_fields is not None and not isinstance(metadata_fields, List):
            raise TypeError(
                f"Expected instance {get_args(signature.parameters.get('metadata_fields').annotation)}, got {type(metadata_fields).__name__}"
            )

        if self._metadata_fields != metadata_fields:
            self._metadata_fields = metadata_fields
            self._filtered_dataset_info = self._filter_dataset_metadata_fields()

        return self._filtered_dataset_info

    def _filter_dataset_metadata_fields(self) -> List[Dict]:
        if self._metadata_fields:
            return [
                {key: item[key] for key in self._metadata_fields if key in item}
                for item in self._datasets_info
            ]
        else:
            return self._datasets_info

    def _get_all_datasets(self) -> List[Dict]:
        self._datasets_info = []

        api_url = self.build_api_url(SEARCH_ENDPOINT)
        response = self._make_api_request(api_url)
        self._datasets_info.extend(response.get("items", []))

        while True:
            if not "next" in response["batching"]:
                break
            response = requests.get(
                response["batching"]["next"], headers={"Accept": "application/json"}
            ).json()

            self._datasets_info.extend(response.get("items", []))

        return self._datasets_info

    def get_api_metadata(self):
        if not self._metadata:
            api_url = self.build_api_url(SEARCH_ENDPOINT)
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

    def get_data_ids(
        self,
    ) -> Union[Iterator[str], Iterator[tuple[str, dict[str, Any]]]]:
        pass
