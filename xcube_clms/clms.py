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
import os
from typing import Any, Container

import xarray as xr
from xcube.core.store import DataTypeLike, DataStore
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
    SCHEMA_KEY,
    PROPERTIES_KEY,
    ALLOWED_SCHEMA_PARAMS,
    FILE_KEY,
    DATA_ID_SEPARATOR,
    ITEM_KEY,
    PRODUCT_KEY,
)
from .preload import PreloadData
from .utils import (
    is_valid_data_type,
    make_api_request,
    get_response_of_type,
    build_api_url,
)


class CLMS:
    def __init__(self, url: str, credentials: dict, path: str | None = None):
        self._url: str = url
        self._datasets_info: list[dict[str, Any]] = []
        self._metadata: list[str] = []
        self.path: str = os.path.join(os.getcwd(), path or "preload_cache/")
        self._preload_data = PreloadData(self._url, credentials, self.path)
        self._fetch_all_datasets()

    @property
    def file_store(self) -> DataStore:
        return self._preload_data.file_store

    def open_data(
        self,
        data_id: str,
        **open_params,
    ) -> xr.Dataset:
        try:
            _, file_id = data_id.split(DATA_ID_SEPARATOR)
        except ValueError as e:
            raise ValueError(
                f"Expected a data_id in the format {{ product_id}}"
                f"{DATA_ID_SEPARATOR}{{file_id}} but got {data_id}"
            )
        if not self.file_store:
            raise ValueError(
                "File store does not exist yet. Please preload "
                "data first using the preload_data() method."
            )

        self._preload_data.refresh_cache()
        cache_entry = self._preload_data.cache.get(data_id)
        if not cache_entry:
            raise FileNotFoundError(f"No cached data found for data_id: {data_id}")

        data_id_file = os.listdir(cache_entry)
        if len(data_id_file) != 1:
            LOG.warning(
                f"Expected 1 file in the folder {cache_entry}, "
                f"got {len(data_id_file)}. Opening the first file."
            )
        return self.file_store.open_data(
            os.path.join(data_id, data_id_file[0]), **open_params
        )

    def get_data_ids(
        self,
        include_attrs: Container[str] | bool | None = None,
    ) -> list[str] | list[tuple[str, dict[str, Any]]]:
        return self._create_data_ids(include_attrs)

    def has_data(self, data_id: str, data_type: DataTypeLike = None) -> bool:
        if is_valid_data_type(data_type):
            dataset = self._get_item(data_id)
            return bool(dataset)
        return False

    def _get_extent(self, data_id: str) -> dict[str, Any]:
        item = self._access_item(data_id.split(DATA_ID_SEPARATOR)[0])
        crs = item.get(CRS_KEY, [])
        time_range = (item.get(START_TIME_KEY), item.get(END_TIME_KEY))

        if len(crs) > 1:
            LOG.warning(
                f"Expected 1 crs, got {len(crs)}. Outputting the first element."
            )

        return dict(time_range=time_range, crs=crs[0] if crs else None)

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
        data_id_maps = {
            data_id: {
                ITEM_KEY: self._access_item(data_id),
                PRODUCT_KEY: self._access_item(data_id.split(DATA_ID_SEPARATOR)[0]),
            }
            for data_id in data_ids
        }
        self._preload_data.initiate_preload(data_id_maps)
        LOG.info(f"Local Filestore created at the path: {self.path}")

    def _create_data_ids(
        self,
        include_attrs: Container[str] | bool | None = None,
    ) -> list[str] | list[tuple[str, dict[str, Any]]]:
        data_ids = []
        for item in self._datasets_info:
            for i in item[DOWNLOADABLE_FILES_KEY][ITEMS_KEY]:
                if FILE_KEY in i and i[FILE_KEY] != "":
                    data_id = (
                        f"{item[CLMS_DATA_ID_KEY]}{DATA_ID_SEPARATOR}{i[FILE_KEY]}"
                    )
                    if not include_attrs or (
                        isinstance(include_attrs, bool) and include_attrs
                    ):
                        data_ids.append(data_id)
                    elif isinstance(include_attrs, list):
                        filtered_attrs = {k: i[k] for k in include_attrs if k in i}
                        data_ids.append(
                            (
                                f"{item[CLMS_DATA_ID_KEY]}{DATA_ID_SEPARATOR}{i[FILE_KEY]}",
                                filtered_attrs,
                            )
                        )
        return data_ids

    def _fetch_all_datasets(self) -> list[dict[str, Any]]:
        if not self._datasets_info:
            LOG.info(f"Fetching datasets metadata from {self._url}")

            response_data = make_api_request(build_api_url(self._url, SEARCH_ENDPOINT))
            while response_data:
                response = get_response_of_type(response_data, JSON_TYPE)
                self._datasets_info.extend(response.get(ITEMS_KEY, []))
                # next_page = response.get(BATCH, {}).get(NEXT)
                # if not next_page:
                #     break
                # response_data = make_api_request(next_page)
                response_data = make_api_request(response.get("next", None))
            self._attrs = list(self._datasets_info[0].keys())
        return self._datasets_info

    def _access_item(self, data_id) -> dict:
        dataset = self._get_item(data_id)
        if len(dataset) != 1:
            raise ValueError(
                f"Expected one dataset for data_id: {data_id}, found"
                f" {len(dataset)}."
            )
        return dataset[0]

    def _get_item(self, data_id):
        if DATA_ID_SEPARATOR in data_id:
            clms_data_product_id, dataset_filename = data_id.split(DATA_ID_SEPARATOR)
            return [
                item
                for product in self._datasets_info
                if product[CLMS_DATA_ID_KEY] == clms_data_product_id
                for item in product.get(DOWNLOADABLE_FILES_KEY, {}).get(ITEMS_KEY, [])
                if item.get("file") == dataset_filename
            ]
        return [
            item for item in self._datasets_info if data_id == item[CLMS_DATA_ID_KEY]
        ]
