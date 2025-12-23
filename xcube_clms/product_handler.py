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

from abc import ABC, abstractmethod
from typing import Any, Container, Iterator

from xcube.core.store import (
    DataDescriptor,
    DataOpener,
    DataPreloader,
    DataStore,
    DataTypeLike,
    PreloadedDataStore,
    PreloadHandle,
)
from xcube.core.store.preload import NullPreloadHandle
from xcube.util.jsonschema import JsonObjectSchema

from .api_token_handler import ClmsApiTokenHandler
from .constants import (
    CLMS_DATA_ID_KEY,
    DATA_ID_SEPARATOR,
    DATASET_DOWNLOAD_INFORMATION,
    FULL_SOURCE,
    ITEMS_KEY,
    SUPPORTED_DATASET_SOURCES,
)
from .product_handlers import get_prod_handlers


class ProductHandler(DataOpener, DataPreloader, ABC):
    """Product handler class to handle various sources of products from the
    CLMS API
    """

    def __init__(
        self,
        cache_store: DataStore = None,
        datasets_info: list[dict] = None,
        api_token_handler: ClmsApiTokenHandler = None,
    ):
        self.cache_store = cache_store
        self.datasets_info = datasets_info
        self.api_token_handler = api_token_handler

    @classmethod
    def guess(
        cls,
        data_id: str,
        datasets_info: list[dict[str, Any]] = None,
        cache_store: PreloadedDataStore = None,
        credentials: dict = None,
    ) -> "ProductHandler":
        """Guess the suitable product handler for the data id requested.

        Args:
            data_id: Data identifier of the data source.
            datasets_info: List of metadata of all datasets from the CLMS API.
                To be passed on to the handlers.
            cache_store: Cache data store to be used by the handlers when
                they use preload_data method. To be passed on to the handlers.
            credentials: Credentials to create a Token handler object to
                refresh the token required to communicate with the CLMS API.
                To be passed on to the handlers.

        Returns:
            The product handler.
        Raises:
            ValueError: if guessing the product handler failed.
        """
        if not all([cache_store, datasets_info, credentials]):
            raise ValueError("All parameters are required")

        def _determine_handler_type():
            for product in datasets_info:
                clms_data_product_id = data_id
                if DATA_ID_SEPARATOR in data_id:
                    clms_data_product_id, dataset_filename = data_id.split(
                        DATA_ID_SEPARATOR
                    )
                if product[CLMS_DATA_ID_KEY] == clms_data_product_id:
                    dataset_download_info = product[DATASET_DOWNLOAD_INFORMATION][
                        ITEMS_KEY
                    ][0]
                    full_source = dataset_download_info.get(FULL_SOURCE)
                    return full_source.lower()
            return None

        handler_type = _determine_handler_type()
        if handler_type is None:
            raise ValueError(
                f"Unable to detect product handler for data_id {data_id!r}."
            )
        if handler_type not in SUPPORTED_DATASET_SOURCES:
            raise ValueError(f"Data source {handler_type} is currently not supported.")
        handler = get_prod_handlers().get(handler_type)
        api_token_handler = ClmsApiTokenHandler(credentials=credentials)
        return handler(
            cache_store=cache_store,
            datasets_info=datasets_info,
            api_token_handler=api_token_handler,
        )

    @abstractmethod
    def get_open_data_params_schema(
        self,
        data_id: str = None,
    ) -> JsonObjectSchema:
        """Get the schema for the parameters passed as *open_params* to
        :meth:`open_data`.
        If *data_id* is given, the returned schema will be tailored
        to the constraints implied by the identified data resource.
        Some openers might not support this, therefore *data_id*
        is optional, and if it is omitted, the returned schema will be
        less restrictive.

        Args:
            data_id: An optional data resource identifier.

        Returns:
            The schema for the parameters in *open_params*.

        Raises:
            DataStoreError: If an error occurs.
        """

    @abstractmethod
    def open_data(
        self,
        data_id: str,
        **open_params,
    ) -> Any:
        """Open the data resource given by the data resource identifier
        *data_id* using the supplied *open_params*.

        Raises if *data_id* does not exist.

        Args:
            data_id: The data resource identifier.
            **open_params: Opener-specific parameters.

        Returns:
            An xarray.Dataset instance.

        Raises:
            DataStoreError: If an error occurs.
        """

    @classmethod
    @abstractmethod
    def product_type(cls) -> str:
        """Returns the product type handled by this class.

        Returns:
            str: The string identifier for the CLMS product type.
        """

    def preload_data(
        self,
        *data_ids: str,
        **preload_params: Any,
    ) -> PreloadHandle:
        return NullPreloadHandle()

    def get_preload_data_params_schema(self) -> JsonObjectSchema:
        return JsonObjectSchema(additional_properties=False)

    @abstractmethod
    def request_download(self, data_id: str) -> list[str]:
        """Requests a download for a given dataset through the CLMS API.

        Args:
            data_id : The dataset identifier to request.

        Returns:
            list[str]: A list containing the task ID for the dataset requested
                which will be tracked and used for next steps or a list of
                URLs that can be lazily loaded.
        """

    @abstractmethod
    def prepare_request(self, data_id: str) -> list[str]:
        """Prepares the API request for accessing the dataset requested.

        NOTE: Include authorization headers.

        Args:
            data_id : The dataset identifier.

        Returns:
            tuple[str, dict]: The URL and headers needed for the request.
        """

    @abstractmethod
    def get_data_id(
        self,
        data_type: DataTypeLike = None,
        include_attrs: Container[str] | bool = False,
        item: dict = None,
    ) -> Iterator[str | tuple[str, dict[str, Any]]]:
        """"""

    @abstractmethod
    def describe_data(self, data_id: str, product: dict) -> DataDescriptor:
        """"""
