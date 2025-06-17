# The MIT License (MIT)
# Copyright (c) 2025 by the xcube development team and contributors
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

from typing import Any, Container, Iterator, Literal

import xarray as xr
from xcube.core.store import DataTypeLike, PreloadedDataStore
from xcube.core.store import new_data_store

from .constants import CLMS_API_URL, SUPPORTED_NON_EEA_DATASET_SOURCES
from .constants import DATA_ID_SEPARATOR
from .constants import DEFAULT_PRELOAD_CACHE_FOLDER
from .constants import ITEM_KEY
from .constants import LOG
from .constants import PRODUCT_KEY
from .constants import SEARCH_ENDPOINT
from .preload import ClmsPreloadHandle
from .utils import build_api_url
from .utils import get_response_of_type
from .utils import is_valid_data_type
from .utils import make_api_request

_CLMS_DATA_ID_KEY = "id"
_DOWNLOADABLE_FILES_KEY = "downloadable_files"
_DATASET_DOWNLOADABLE = "downloadable_full_dataset"
_DATASET_DOWNLOAD_INFORMATION = "dataset_download_information"
_FULL_SOURCE = "full_source"
_FILE_KEY = "file"
_CRS_KEY = "coordinateReferenceSystemList"
_START_TIME_KEY = "temporalExtentStart"
_END_TIME_KEY = "temporalExtentEnd"
_BATCH = "batching"
_NEXT = "next"
_ITEMS_KEY = "items"


class Clms:
    """Provides an interface to interact with the CLMS API from the
    ClmsDataStore
    """

    def __init__(
        self,
        credentials: dict,
        cache_store_id: str = "file",
        cache_store_params: dict | None = None,
    ) -> None:
        if cache_store_params is None or cache_store_params.get("root") is None:
            cache_store_params = dict(root=DEFAULT_PRELOAD_CACHE_FOLDER)
        cache_store_params["max_depth"] = cache_store_params.pop("max_depth", 2)
        self.cache_store: PreloadedDataStore = new_data_store(
            cache_store_id, **cache_store_params
        )
        self.cache_store_id = cache_store_id
        self.fs = self.cache_store.fs
        self._cache_root = self.cache_store.root
        self.credentials = credentials
        self._datasets_info: list[dict[str, Any]] = Clms._fetch_all_datasets()

    def open_data(
        self,
        data_id: str,
        opener_id: str = None,
        **open_params,
    ) -> xr.Dataset:
        """Opens the data associated with a specific data ID from the cache
        store.

        Args:
            data_id: Identifier for the data to open.
            opener_id: Identifier for the data opener.
            **open_params: Additional parameters for opening the data.

        Returns:
            The opened data object.

        Raises:
            FileNotFoundError: If the data ID is not found in the cache.
        """
        if not self.cache_store.has_data(data_id):
            raise FileNotFoundError(
                f"No cached data found for data_id: "
                f"{data_id}. Please preload the data "
                f"first using the `preload_data()` method."
            )

        return self.cache_store.open_data(
            data_id=data_id, opener_id=opener_id, **open_params
        )

    def get_data_ids(
        self,
        include_attrs: Container[str] | bool = False,
    ) -> Iterator[str | tuple[str, dict[str, Any]]]:
        """Retrieves all data IDs, optionally including additional attributes.

        Args:
            include_attrs: Specifies whether to include attributes for each
               `data_id`.
                - If True, includes all attributes.
                - If a list, includes specified attributes.
                - If False (default), includes no attributes.

        Returns:
            An iterator of data IDs, or tuples of data IDs and attributes.
        """
        for item in self._datasets_info:
            if item[_DATASET_DOWNLOADABLE]:
                if len(item[_DATASET_DOWNLOAD_INFORMATION]) > 0:
                    dataset_download_info = item[_DATASET_DOWNLOAD_INFORMATION][
                        _ITEMS_KEY
                    ][0]
                    if dataset_download_info[_FULL_SOURCE] == "EEA":
                        for i in item[_DOWNLOADABLE_FILES_KEY][_ITEMS_KEY]:
                            if _FILE_KEY in i and i[_FILE_KEY] != "":
                                data_id = f"{item[_CLMS_DATA_ID_KEY]}{DATA_ID_SEPARATOR}{i[_FILE_KEY]}"
                                if not include_attrs:
                                    yield data_id
                                elif isinstance(include_attrs, bool) and include_attrs:
                                    yield data_id, i
                                elif isinstance(include_attrs, list):
                                    filtered_attrs = {
                                        attr: i[attr]
                                        for attr in include_attrs
                                        if attr in i
                                    }
                                    yield data_id, filtered_attrs
                    elif (
                        dataset_download_info[_FULL_SOURCE]
                        in SUPPORTED_NON_EEA_DATASET_SOURCES
                    ):
                        data_id = f"{item["id"]}"
                        if not include_attrs:
                            yield data_id
                        elif isinstance(include_attrs, bool) and include_attrs:
                            yield data_id, dataset_download_info
                        elif isinstance(include_attrs, list):
                            filtered_attrs = {
                                attr: dataset_download_info[attr]
                                for attr in include_attrs
                                if attr in dataset_download_info
                            }
                            yield data_id, filtered_attrs

    def has_data(self, data_id: str, data_type: DataTypeLike = None) -> bool:
        """Checks if data exists for the given data ID and optional type.

        Args:
            data_id: Identifier for the data to check.
            data_type: Optional type to validate against.

        Returns:
            True if data exists, False otherwise.
        """
        if is_valid_data_type(data_type):
            if DATA_ID_SEPARATOR in data_id:
                dataset = self._extract_dataset_component(data_id, item_type="item")
            else:
                dataset = self._extract_dataset_component(data_id, item_type="product")
            return bool(dataset)
        return False

    def describe_data(self, data_id: str) -> dict[str, Any]:
        """Get the time range and CRS of the dataset.

        Args:
            data_id: Identifier for the dataset.

        Returns:
            A dictionary with the dataset's time range and CRS (currently
            supported).
        """
        item = self._get_extracted_component(data_id)
        crs = item.get(_CRS_KEY, [])
        time_range = (item.get(_START_TIME_KEY), item.get(_END_TIME_KEY))

        if len(crs) > 1:
            LOG.warning(
                f"Expected 1 crs, got {len(crs)}. Outputting the first element."
            )

        return dict(time_range=time_range, crs=crs[0] if crs else None)

    def preload_data(self, *data_ids: str, **preload_params) -> PreloadedDataStore:
        """Preloads the data into a cache for specified data IDs with optional
        parameters for faster access when using the `open_data` method.

        Args:
            *data_ids: One or more data IDs to preload.
            **preload_params: Parameters for preloading data into a cache
                data store. Use `get_preload_data_params_schema()` to get all
                the parameters that can be passed in.
        """
        data_id_maps = {
            data_id: {
                ITEM_KEY: self._get_extracted_component(data_id, item_type="item"),
                PRODUCT_KEY: self._get_extracted_component(data_id),
            }
            for data_id in data_ids
        }
        # noinspection PyPropertyAccess
        self.cache_store.preload_handle = ClmsPreloadHandle(
            data_id_maps=data_id_maps,
            url=CLMS_API_URL,
            credentials=self.credentials,
            cache_store=self.cache_store,
            **preload_params,
        )
        return self.cache_store

    @staticmethod
    def _fetch_all_datasets() -> list[dict[str, Any]]:
        """Fetches all datasets from the API and caches their metadata.

        Returns:
            A list of dictionaries representing all datasets.
        """
        LOG.info(f"Fetching datasets metadata from {CLMS_API_URL}")
        datasets_info = []
        response_data = make_api_request(
            build_api_url(CLMS_API_URL, SEARCH_ENDPOINT, datasets_request=True)
        )
        while True:
            response = get_response_of_type(response_data, "json")
            datasets_info.extend(response.get(_ITEMS_KEY, []))
            next_page = response.get(_BATCH, {}).get(_NEXT)
            if not next_page:
                break
            response_data = make_api_request(next_page)
        LOG.info("Fetching complete.")
        return datasets_info

    def _get_extracted_component(
        self, data_id, item_type: Literal["item", "product"] = "product"
    ) -> dict[str, Any]:
        """Extracts either an item or product from the list of datasets
        available

        Args:
            data_id: The unique identifier for the dataset.

        Returns:
            A dictionary representing the dataset item.

        Raises:
            ValueError: If the dataset item is not found or multiple items match.
        """
        dataset = self._extract_dataset_component(data_id, item_type)
        if len(dataset) != 1:
            raise ValueError(
                f"Expected one dataset for data_id: {data_id}, found"
                f" {len(dataset)}."
            )
        return dataset[0]

    def _extract_dataset_component(
        self, data_id: str, item_type: Literal["item", "product"]
    ) -> list[dict[str, Any]] | list[Any]:
        """Extracts either a dataset product or its downloadable item(s) for a
        given data ID.

        Args:
            data_id: Identifier for the dataset or its components.
            item_type: 'item' to return downloadable item(s), 'product' to
                return the dataset entry.

        Returns:
            A list of dictionaries for items or products, depending on item_type.
        """
        if item_type == "item":
            if DATA_ID_SEPARATOR in data_id:
                clms_data_product_id, dataset_filename = data_id.split(
                    DATA_ID_SEPARATOR
                )
                return [
                    item
                    for product in self._datasets_info
                    if product[_CLMS_DATA_ID_KEY] == clms_data_product_id
                    for item in product.get(_DOWNLOADABLE_FILES_KEY, {}).get(
                        _ITEMS_KEY, []
                    )
                    if item.get("file") == dataset_filename
                ]

            for product in self._datasets_info:
                if product[_CLMS_DATA_ID_KEY] == data_id:
                    dataset_download_info = product[_DATASET_DOWNLOAD_INFORMATION][
                        _ITEMS_KEY
                    ][0]
                    if (
                        dataset_download_info.get(_FULL_SOURCE)
                        in SUPPORTED_NON_EEA_DATASET_SOURCES
                    ):
                        return [dataset_download_info]

            return []

        elif item_type == "product":
            return [
                product
                for product in self._datasets_info
                if data_id.split(DATA_ID_SEPARATOR)[0] == product[_CLMS_DATA_ID_KEY]
            ]

        else:
            raise ValueError(
                f"Invalid item_type: {item_type}. Must be 'item' or 'product'."
            )
