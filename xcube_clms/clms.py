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
from typing import Any, get_args, Container

import xarray as xr
from xcube.core.store import DataTypeLike
from xcube.util.jsonschema import JsonObjectSchema, JsonStringSchema

from .constants import (
    SEARCH_ENDPOINT,
    LOG,
    CLMS_DATA_ID_KEY,
    DOWNLOADABLE_FILES_KEY,
    ITEMS_KEY,
    CRS_KEY,
    START_TIME_KEY,
    END_TIME_KEY,
    JSON_TYPE,
    BATCH,
    NEXT,
    SCHEMA_KEY,
    PROPERTIES_KEY,
    ALLOWED_SCHEMA_PARAMS,
    FILE_KEY,
    TASK_IDS_KEY,
    DOWNLOAD_URL_KEY,
)
from .preload import PreloadData
from .utils import (
    is_valid_data_type,
    make_api_request,
    get_response_of_type,
    build_api_url,
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
        credentials: dict,
    ):
        self._api_token = None
        self._clms_api_token_instance = None
        self._url = url
        self._datasets_info: list[dict[str, Any]] = []
        self._metadata: list[str] = []
        self._data_ids = []
        self._preload_data = PreloadData(self._url, credentials)
        self.download_url: str = ""

    def open_dataset(
        self,
        data_id: str,
        spatial_coverage: str = "",
        resolution: str = "",
        title: str = "",
        **open_params,
    ) -> xr.Dataset:
        raise NotImplementedError()

    def get_data_ids(
        self,
        include_attrs: Container[str] | bool | None = None,
    ) -> list[str] | list[tuple[str, dict[str, Any]]]:
        return self._create_data_ids(include_attrs)

    def has_data(self, data_id: str, data_type: DataTypeLike = None) -> bool:
        if is_valid_data_type(data_type):
            self._fetch_all_datasets()
            dataset = self._get_item(data_id)
            if len(dataset) == 0:
                return False
            return True
        return False

    def get_extent(self, data_id: str) -> dict:
        self._fetch_all_datasets()
        item = self._access_item(data_id.split(":")[0])
        crs = item.get(CRS_KEY)
        time_range = (item.get(START_TIME_KEY), item.get(END_TIME_KEY))

        if len(crs) > 1:
            LOG.warning(
                f"Expected 1 crs, got {len(crs)}. Outputting the first element."
            )

        return dict(time_range=time_range, crs=crs[0])

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

    def preload_data(self, *data_ids: str, **preload_params):
        task_ids = {}
        for data_id in data_ids:
            item = self._access_item(data_id)
            product = self._access_item(data_id.split(":")[0])
            task_id, download_url = self._preload_data.queue_download(
                data_id, item, product
            )
            task_ids[TASK_IDS_KEY] = task_id
            task_ids[DOWNLOAD_URL_KEY] = download_url
        print(task_ids)
        # TODO: Check for queued datasets if they are available for download
        #  If they are, create one filestore in a location provided by preload_params and use it as the dir to store the downloaded dataset
        #  Create a progress bar that indicates the queued downloads, download process, and postprocess to create the cube

    def _create_data_ids(
        self,
        include_attrs: Container[str] | bool | None = None,
    ) -> list[str] | list[tuple[str, dict[str, Any]]]:
        if not self._datasets_info:
            self._fetch_all_datasets()

        data_ids = []
        for item in self._datasets_info:
            for i in item[DOWNLOADABLE_FILES_KEY][ITEMS_KEY]:
                if FILE_KEY in i and i[FILE_KEY] != "":
                    if not include_attrs:
                        data_ids.append(f"{item[CLMS_DATA_ID_KEY]}:{i[FILE_KEY]}")
                    elif isinstance(include_attrs, bool) and include_attrs:
                        data_ids.append((f"{item[CLMS_DATA_ID_KEY]}:{i[FILE_KEY]}", i))
                    elif isinstance(include_attrs, list):
                        attrs = {}
                        for attr in include_attrs:
                            if attr in i:
                                attrs[attr] = i[attr]
                        data_ids.append(
                            (f"{item[CLMS_DATA_ID_KEY]}:{i[FILE_KEY]}", attrs)
                        )
        return data_ids

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

            response_data = make_api_request(build_api_url(self._url, SEARCH_ENDPOINT))
            while True:
                response = get_response_of_type(response_data, JSON_TYPE)
                self._datasets_info.extend(response.get(ITEMS_KEY, []))
                next_page = response.get(BATCH, {}).get(NEXT)
                if not next_page:
                    break
                response_data = make_api_request(next_page)
            self._attrs = list(self._datasets_info[0].keys())
        return self._datasets_info

    def _access_item(self, data_id) -> dict:
        dataset = self._get_item(data_id)
        if len(dataset) > 1:
            raise Exception(
                f"Expected one item for data_id: {data_id}, found {len(dataset)}."
            )
        if len(dataset) == 0:
            raise Exception(f"Data id: {data_id} not found in the CLMS catalog")
        return dataset[0]

    def _get_item(self, data_id):
        if len(data_id.split(":")) == 2:
            clms_data_product_id, dataset_filename = data_id.split(":")
            dataset = []
            for product in self._datasets_info:
                for item in product.get(DOWNLOADABLE_FILES_KEY).get(ITEMS_KEY):
                    if (
                        item.get("file") == dataset_filename
                        and product.get(CLMS_DATA_ID_KEY) == clms_data_product_id
                    ):
                        dataset.append(item)
        else:
            dataset = [
                item
                for item in self._datasets_info
                if data_id == item[CLMS_DATA_ID_KEY]
            ]
        return dataset
