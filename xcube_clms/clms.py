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
from xcube.core.store import DataTypeLike

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
    FILE_KEY,
    DATA_ID_SEPARATOR,
    ITEM_KEY,
    PRODUCT_KEY,
    BATCH,
    NEXT,
    PRELOAD_CACHE_FOLDER,
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
        """Initialize the CLMS class with API URL, credentials, and optional path.

        Args:
            url: Base URL for the API.
            credentials: JSON containing authentication credentials.
            path: Optional cache path for storing preloaded data.
        """
        self._url: str = url
        self._metadata: list[str] = []
        self.path: str = os.path.join(os.getcwd(), path or PRELOAD_CACHE_FOLDER)
        self._preload_data = PreloadData(self._url, credentials, self.path)
        self.file_store = self._preload_data.file_store
        self._datasets_info: list[dict[str, Any]] = CLMS._fetch_all_datasets(self._url)

    def open_data(
        self,
        data_id: str,
        **open_params,
    ) -> xr.Dataset:
        """Open data associated with a specific data ID.

        Args:
            data_id: Identifier for the data to open.
            **open_params: Additional parameters for opening the data.

        Returns:
            The opened data object.

        Raises:
            ValueError: If the data ID is invalid or improperly formatted.
            FileNotFoundError: If the data ID is not found in the cache.
        """

        try:
            _, file_id = data_id.split(DATA_ID_SEPARATOR)
        except ValueError as e:
            raise ValueError(
                f"Expected a data_id in the format {{product_id}}"
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
        """Retrieve all data IDs, optionally including additional attributes.

        Args:
            include_attrs: Specifies whether to include attributes.
                - If True, includes all attributes.
                - If a list, includes specified attributes.
                - If False or None, includes no attributes.

        Returns:
            A list of data IDs, or tuples of data IDs and attributes.
        """
        return self._create_data_ids(include_attrs)

    def has_data(self, data_id: str, data_type: DataTypeLike = None) -> bool:
        """Check if data exists for the given data ID and optional type.

        Args:
            data_id: Identifier for the data to check.
            data_type: Optional type to validate against.

        Returns:
            True if data exists, False otherwise.
        """
        if is_valid_data_type(data_type):
            dataset = self._get_item(data_id)
            return bool(dataset)
        return False

    def _get_extent(self, data_id: str) -> dict[str, Any]:
        """Retrieve the spatial and temporal extent of a dataset.

        Args:
            data_id: Identifier for the dataset.

        Returns:
            A dictionary with the dataset's time range and CRS (currently
            supported).
        """
        item = self._access_item(data_id.split(DATA_ID_SEPARATOR)[0])
        crs = item.get(CRS_KEY, [])
        time_range = (item.get(START_TIME_KEY), item.get(END_TIME_KEY))

        if len(crs) > 1:
            LOG.warning(
                f"Expected 1 crs, got {len(crs)}. Outputting the first element."
            )

        return dict(time_range=time_range, crs=crs[0] if crs else None)

    def preload_data(self, *data_ids: str, **preload_params):
        """Preload data into a cache for specified data IDs with optional
        parameters for faster access using open_data.

        Args:
            *data_ids: One or more data IDs to preload.
            **preload_params: Additional parameters for preloading (currently
            not supported).

        Returns:
            None.

        Raises:
            ValueError: If any data ID is invalid.
        """
        data_id_maps = {
            data_id: {
                ITEM_KEY: self._access_item(data_id),
                PRODUCT_KEY: self._access_item(data_id.split(DATA_ID_SEPARATOR)[0]),
            }
            for data_id in data_ids
        }
        self._preload_data.initiate_preload(data_id_maps)

    def _create_data_ids(
        self,
        include_attrs: Container[str] | bool | None = None,
    ) -> list[str] | list[tuple[str, dict[str, Any]]]:
        """
        Generates a list of data IDs, optionally including attributes. This is
        the actual implementation of the get_data_ids() method.

        Args:
            include_attrs: Specifies whether to include attributes.
                - If True, includes all attributes.
                - If a list, includes specified attributes.
                - If False or None, includes no attributes.

        Returns:
            A list of data IDs or tuples of data IDs and attributes.
        """
        data_ids = []
        for item in self._datasets_info:
            for i in item[DOWNLOADABLE_FILES_KEY][ITEMS_KEY]:
                if FILE_KEY in i and i[FILE_KEY] != "":
                    data_id = (
                        f"{item[CLMS_DATA_ID_KEY]}{DATA_ID_SEPARATOR}{i[FILE_KEY]}"
                    )
                    if not include_attrs:
                        data_ids.append(data_id)
                    elif isinstance(include_attrs, bool) and include_attrs:
                        data_ids.append((data_id, i))
                    elif isinstance(include_attrs, list):
                        filtered_attrs = {
                            attr: i[attr] for attr in include_attrs if attr in i
                        }
                        data_ids.append((data_id, filtered_attrs))
        return data_ids

    @staticmethod
    def _fetch_all_datasets(url) -> list[dict[str, Any]]:
        """Fetch all datasets from the API and cache their metadata.

        Returns:
            A list of dictionaries representing all datasets.

        Raises:
            RequestException: If an error occurs during the API request.
            Other exceptions such as Timeout, JSONDecodeError could be raised
            as well from the make_api_request()
            ValueError: For type mismatches between response object type and
            the required type in get_response_of_type()
            TypeError: For invalid input to get_response_of_type()
        """
        LOG.info(f"Fetching datasets metadata from {url}")
        datasets_info = []
        response_data = make_api_request(build_api_url(url, SEARCH_ENDPOINT))
        while True:
            response = get_response_of_type(response_data, JSON_TYPE)
            datasets_info.extend(response.get(ITEMS_KEY, []))
            next_page = response.get(BATCH, {}).get(NEXT)
            if not next_page:
                break
            response_data = make_api_request(next_page)
        return datasets_info

    def _access_item(self, data_id) -> dict[str, Any]:
        """Access an item from the dataset for a given data ID.

        Args:
            data_id: The unique identifier for the dataset.

        Returns:
            A dictionary representing the dataset item.

        Raises:
            ValueError: If the dataset item is not found or multiple items match.
        """
        dataset = self._get_item(data_id)
        if len(dataset) != 1:
            raise ValueError(
                f"Expected one dataset for data_id: {data_id}, found"
                f" {len(dataset)}."
            )
        return dataset[0]

    def _get_item(self, data_id: str) -> list[dict[str, Any]] | list[any]:
        """Retrieve a dataset item or its components for a given data ID.
        This is the actual implementation of the _access_item() method

        Args:
            data_id: Identifier for the dataset or its components.

        Returns:
            A list of dictionaries matching the data ID.
        """
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
