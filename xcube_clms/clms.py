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
from typing import Any, Container, Union, Iterator

import xarray as xr
from xcube.core.store import DataTypeLike, PreloadHandle

from .constants import (
    SEARCH_ENDPOINT,
    LOG,
    DATA_ID_SEPARATOR,
    DEFAULT_PRELOAD_CACHE_FOLDER,
    CLMS_API_URL,
)
from .preload import ClmsPreloadHandle
from .utils import (
    is_valid_data_type,
    make_api_request,
    get_response_of_type,
    build_api_url,
)

_CLMS_DATA_ID_KEY = "id"
_DOWNLOADABLE_FILES_KEY = "downloadable_files"
_FILE_KEY = "file"
_CRS_KEY = "coordinateReferenceSystemList"
_START_TIME_KEY = "temporalExtentStart"
_END_TIME_KEY = "temporalExtentEnd"
_ITEM_KEY = "item"
_PRODUCT_KEY = "product"
_BATCH = "batching"
_NEXT = "next"
_ITEMS_KEY = "items"


class Clms:
    """Provides an interface to interact with the CLMS API

    It also allows the user to preload the data into a cache location in a
    non-blocking way which would be time-consuming task otherwise using the
    `preload_data` method. Currently, this is experimental and will change in
    the further versions.
    """

    def __init__(
        self,
        credentials: dict,
        path: str | None = None,
        cleanup: bool | None = None,
        disable_tqdm_progress: bool | None = None,
    ) -> None:
        self.path: str = os.path.join(os.getcwd(), path or DEFAULT_PRELOAD_CACHE_FOLDER)
        self.credentials = credentials
        self.cleanup = cleanup
        self.disable_tqdm_progress = disable_tqdm_progress
        self._datasets_info: list[dict[str, Any]] = Clms._fetch_all_datasets()
        self.preload_handle: ClmsPreloadHandle | None = None

    def open_data(
        self,
        data_id: str,
        **open_params,
    ) -> xr.Dataset:
        """Opens the data associated with a specific data ID.

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
            LOG.error(
                f"The format of the data ID is wrong. Expected it in "
                f"the format {{product_id}}{DATA_ID_SEPARATOR}{{file_id}} but "
                f"got {data_id}. {e}"
            )
            raise
        if not self.preload_handle.data_store:
            raise ValueError(
                "Cache data store does not exist yet. Please preload "
                "data first using the preload_data() method."
            )

        self.preload_handle.refresh_cache()
        cache_entry = self.preload_handle.view_cache().get(data_id)
        if not cache_entry:
            raise FileNotFoundError(f"No cached data found for data_id: {data_id}")

        data_id_file = os.listdir(cache_entry)
        if len(data_id_file) != 1:
            LOG.warning(
                f"Expected 1 file in the folder {cache_entry}, "
                f"got {len(data_id_file)}. Opening the first file."
            )
        return self.preload_handle.data_store.open_data(
            os.path.join(data_id, data_id_file[0]), **open_params
        )

    def get_data_ids(
        self,
        include_attrs: Container[str] | bool | None = None,
    ) -> Union[Iterator[str], Iterator[tuple[str, dict[str, Any]]]]:
        """Retrieves all data IDs, optionally including additional attributes.

        Args:
            include_attrs: Specifies whether to include attributes.
                - If True, includes all attributes.
                - If a list, includes specified attributes.
                - If False or None, includes no attributes.

        Returns:
            An iterator of data IDs, or tuples of data IDs and attributes.
        """
        for data_id in self._create_data_ids(include_attrs):
            yield data_id

    def has_data(self, data_id: str, data_type: DataTypeLike = None) -> bool:
        """Checks if data exists for the given data ID and optional type.

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

    def get_extent(self, data_id: str) -> dict[str, Any]:
        """Retrieves the spatial and temporal extent of a dataset.

        Args:
            data_id: Identifier for the dataset.

        Returns:
            A dictionary with the dataset's time range and CRS (currently
            supported).
        """
        item = self._access_item(data_id.split(DATA_ID_SEPARATOR)[0])
        crs = item.get(_CRS_KEY, [])
        time_range = (item.get(_START_TIME_KEY), item.get(_END_TIME_KEY))

        if len(crs) > 1:
            LOG.warning(
                f"Expected 1 crs, got {len(crs)}. Outputting the first element."
            )

        return dict(time_range=time_range, crs=crs[0] if crs else None)

    def preload_data(
        self, *data_ids: str, blocking: bool, silent: bool, **preload_params
    ) -> PreloadHandle:
        """Preloads the data into a cache for specified data IDs with optional
        parameters for faster access when using the `open_data` method.

        Args:
            *data_ids: One or more data IDs to preload.
            **preload_params: Additional parameters for preloading (currently
            not supported).

        Raises:
            ValueError: If any data ID is invalid.
        """
        data_id_maps = {
            data_id: {
                _ITEM_KEY: self._access_item(data_id),
                _PRODUCT_KEY: self._access_item(data_id.split(DATA_ID_SEPARATOR)[0]),
            }
            for data_id in data_ids
        }

        return ClmsPreloadHandle(
            data_id_maps=data_id_maps,
            blocking=blocking,
            silent=silent,
            url=CLMS_API_URL,
            credentials=self.credentials,
            path=self.path,
            cleanup=self.cleanup,
            disable_tqdm_progress=self.disable_tqdm_progress,
        )

    def _create_data_ids(
        self,
        include_attrs: Container[str] | bool | None = None,
    ) -> Union[Iterator[str], Iterator[tuple[str, dict[str, Any]]]]:
        """Generates a list of data IDs, optionally including attributes.

        Args:
            include_attrs: Specifies whether to include attributes.
                - If True, includes all attributes.
                - If a list, includes specified attributes.
                - If False or None, includes no attributes.

        Returns:
            An iterator of data IDs or tuples of data IDs and attributes.
        """
        for item in self._datasets_info:
            for i in item[_DOWNLOADABLE_FILES_KEY][_ITEMS_KEY]:
                if _FILE_KEY in i and i[_FILE_KEY] != "":
                    data_id = (
                        f"{item[_CLMS_DATA_ID_KEY]}{DATA_ID_SEPARATOR}{i[_FILE_KEY]}"
                    )
                    if not include_attrs:
                        yield data_id
                    elif isinstance(include_attrs, bool) and include_attrs:
                        yield data_id, i
                    elif isinstance(include_attrs, list):
                        filtered_attrs = {
                            attr: i[attr] for attr in include_attrs if attr in i
                        }
                        yield data_id, filtered_attrs

    @staticmethod
    def _fetch_all_datasets() -> list[dict[str, Any]]:
        """Fetches all datasets from the API and caches their metadata.

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
        LOG.info(f"Fetching datasets metadata from {CLMS_API_URL}")
        datasets_info = []
        response_data = make_api_request(build_api_url(CLMS_API_URL, SEARCH_ENDPOINT))
        while True:
            response = get_response_of_type(response_data, "json")
            datasets_info.extend(response.get(_ITEMS_KEY, []))
            next_page = response.get(_BATCH, {}).get(_NEXT)
            if not next_page:
                break
            response_data = make_api_request(next_page)
        return datasets_info

    def _access_item(self, data_id) -> dict[str, Any]:
        """Accesses an item from the dataset for a given data ID.

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
        """Retrieves a dataset item or its components for a given data ID.

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
                if product[_CLMS_DATA_ID_KEY] == clms_data_product_id
                for item in product.get(_DOWNLOADABLE_FILES_KEY, {}).get(_ITEMS_KEY, [])
                if item.get("file") == dataset_filename
            ]
        return [
            item for item in self._datasets_info if data_id == item[_CLMS_DATA_ID_KEY]
        ]
