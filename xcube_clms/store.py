# The MIT License (MIT)
# Copyright (c) 2025 by the xcube development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
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

from typing import Any, Container, Iterator, Tuple

import xarray as xr
from xcube.core.store import (
    DATASET_TYPE,
    DataDescriptor,
    DataStore,
    DataStoreError,
    DataTypeLike,
    PreloadedDataStore,
    new_data_store,
)
from xcube.util.jsonschema import (
    JsonArraySchema,
    JsonBooleanSchema,
    JsonComplexSchema,
    JsonIntegerSchema,
    JsonObjectSchema,
    JsonStringSchema,
)

from .api_token_handler import ClmsApiTokenHandler
from .constants import (
    DATASET_DOWNLOAD_INFORMATION,
    DEFAULT_PRELOAD_CACHE_FOLDER,
    FULL_SOURCE,
    ITEMS_KEY,
    LOG,
    EEA,
    CDSE,
)
from .product_handler import ProductHandler
from .product_handlers import get_prod_handlers
from .product_handlers.eea import EeaProductHandler
from .utils import (
    assert_valid_data_type,
    fetch_all_datasets,
    get_extracted_component,
)

_CRS_KEY = "coordinateReferenceSystemList"
_START_TIME_KEY = "temporalExtentStart"
_END_TIME_KEY = "temporalExtentEnd"
_DATASET_DOWNLOADABLE = "downloadable_full_dataset"

_source_key_map = {
    EEA.upper(): EEA,
    CDSE.upper(): CDSE,
}


class ClmsDataStore(DataStore):
    """CLMS implementation of the data store defined in the ``xcube_clms``
    plugin."""

    def __init__(
        self,
        credentials: dict,
        cache_store_id: str = "file",
        cache_store_params: dict | None = None,
    ):
        self.cache_store_params = cache_store_params
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
        self.data_opener_id = (
            f"dataset:zarr:{self.cache_store.protocol}",
            "dataset:netcdf:https",
        )
        self._datasets_info: list[dict[str, Any]] = []

    @classmethod
    def get_data_store_params_schema(cls) -> JsonObjectSchema:
        credentials_params = dict(
            client_id=JsonStringSchema(),
            issued=JsonStringSchema(),
            private_key=JsonStringSchema(),
            key_id=JsonStringSchema(),
            title=JsonStringSchema(),
            token_uri=JsonStringSchema(),
            user_id=JsonStringSchema(),
        )

        params = dict(
            credentials=JsonObjectSchema(
                dict(**credentials_params),
                title="CLMS API credentials that can be obtained following "
                "the steps outlined here. "
                "https://eea.github.io/clms-api-docs/authentication.html",
                required=("client_id", "user_id", "token_uri", "private_key"),
            ),
            cache_store_id=JsonStringSchema(
                title="Store ID of cache data store.",
                description=(
                    "Store ID of a filesystem-based data store implemented in "
                    ""
                    "xcube."
                ),
                default="file",
            ),
            cache_store_params=JsonObjectSchema(
                title="Store parameters of cache data store.",
                description=(
                    "Parameters of a filesystem-based data store implemented "
                    "in xcube. "
                    "Provide parameters for a file data store if "
                    "`cache_store_id` is not provided."
                ),
                default=dict(root=DEFAULT_PRELOAD_CACHE_FOLDER),
            ),
        )
        return JsonObjectSchema(
            properties=dict(**params),
            required=("credentials",),
            additional_properties=False,
        )

    @classmethod
    def get_data_types(cls) -> Tuple[str, ...]:
        return (DATASET_TYPE.alias,)

    def get_data_types_for_data(self, data_id: str) -> Tuple[str, ...]:
        return self.get_data_types()

    def get_data_ids(
        self,
        data_type: DataTypeLike = None,
        include_attrs: Container[str] | bool = False,
    ) -> Iterator[str | tuple[str, dict[str, Any]]]:
        if len(self._datasets_info) == 0:
            self._datasets_info = fetch_all_datasets()
        assert_valid_data_type(data_type)
        prod_handlers = get_prod_handlers()
        _api_token_handler = ClmsApiTokenHandler(credentials=self.credentials)
        for item in self._datasets_info:
            if len(item[DATASET_DOWNLOAD_INFORMATION][ITEMS_KEY]) > 0:
                dataset_download_info = item[DATASET_DOWNLOAD_INFORMATION][ITEMS_KEY][0]
                source = dataset_download_info.get(FULL_SOURCE)
                if not source:
                    continue
                handler_key = _source_key_map.get(source)
                if handler_key is None:
                    LOG.debug(f"source {source} is not supported.")
                    continue
                handler = prod_handlers.get(handler_key)
                if handler is None:
                    LOG.debug(f"handler {handler_key} is not supported.")
                    continue
                for data_id_and_maybe_attrs in handler(
                    self._datasets_info, self.cache_store, _api_token_handler
                ).get_data_id(
                    data_type=data_type, include_attrs=include_attrs, item=item
                ):
                    if include_attrs:
                        yield (
                            data_id_and_maybe_attrs[0],
                            data_id_and_maybe_attrs[1],
                        )
                    else:
                        yield data_id_and_maybe_attrs

    def has_data(self, data_id: str, data_type: DataTypeLike = None) -> bool:
        if len(self._datasets_info) == 0:
            self._datasets_info = fetch_all_datasets()
        return data_id in self.list_data_ids()

    def describe_data(
        self, data_id: str, data_type: DataTypeLike = None
    ) -> DataDescriptor:
        assert_valid_data_type(data_type)
        if data_id is None:
            raise ValueError("Please provide a valid data ID.")
        if len(self._datasets_info) == 0:
            self._datasets_info = fetch_all_datasets()

        handler = ProductHandler.guess(
            data_id,
            self._datasets_info,
            self.cache_store,
            self.credentials,
        )
        product = get_extracted_component(self._datasets_info, data_id)
        return handler.describe_data(data_id=data_id, product=product)

    def get_data_opener_ids(
        self, data_id: str = None, data_type: DataTypeLike = None
    ) -> Tuple[str, ...]:
        return self.data_opener_id

    def get_open_data_params_schema(
        self, data_id: str = None, opener_id: str = None
    ) -> JsonObjectSchema:
        if opener_id:
            self._assert_valid_opener_id(opener_id)
        if len(self._datasets_info) == 0:
            self._datasets_info = fetch_all_datasets()
        if data_id is not None:
            handler = ProductHandler.guess(
                data_id,
                self._datasets_info,
                self.cache_store,
                self.credentials,
            )
            return handler.get_open_data_params_schema(data_id)
        elif opener_id is not None:
            if opener_id == "dataset:zarr:file":
                return get_prod_handlers()["eea"](
                    self._datasets_info, self.cache_store, self.credentials
                ).get_open_data_params_schema()
            else:
                _api_token_handler = ClmsApiTokenHandler(credentials=self.credentials)
                return JsonObjectSchema(
                    title="Opening parameters for all supported CLMS products "
                    "except EEA.",
                    properties={
                        key: ph(
                            self._datasets_info,
                            self.cache_store,
                            _api_token_handler,
                        ).get_open_data_params_schema()
                        for (key, ph) in get_prod_handlers().items()
                        if key != EEA.lower()
                    },
                )
        else:
            _api_token_handler = ClmsApiTokenHandler(credentials=self.credentials)
            return JsonObjectSchema(
                title="Opening parameters for all supported CLMS products.",
                properties={
                    key: ph(
                        self._datasets_info, self.cache_store, _api_token_handler
                    ).get_open_data_params_schema()
                    for (key, ph) in get_prod_handlers().items()
                },
            )

    def _assert_valid_opener_id(self, opener_id: str) -> None:
        if opener_id is not None and opener_id not in self.data_opener_id:
            raise DataStoreError(
                f"Data opener identifiers must be {self.data_opener_id!r}, "
                f"but got {opener_id!r}."
            )

    def open_data(
        self,
        data_id: str,
        opener_id: str = None,
        **open_params,
    ) -> xr.Dataset:
        schema = self.get_open_data_params_schema(data_id)
        schema.validate_instance(open_params)
        if not self.has_data(data_id):
            raise ValueError(f"The requested data_id {data_id} is invalid.")
        handler = ProductHandler.guess(
            data_id,
            self._datasets_info,
            self.cache_store,
            self.credentials,
        )
        if isinstance(handler, EeaProductHandler):
            raise ValueError(
                f"The requested data_id {data_id} cannot be "
                f"opened. Try using preload_data()"
            )
        return handler.open_data(data_id, **open_params)

    def search_data(
        self, data_type: DataTypeLike = None, **search_params
    ) -> Iterator[DataDescriptor]:
        raise NotImplementedError("search_data() operation is not supported yet")

    @classmethod
    def get_search_params_schema(
        cls, data_type: DataTypeLike = None
    ) -> JsonObjectSchema:
        return JsonObjectSchema()

    def preload_data(
        self,
        *data_ids: str,
        **preload_params,
    ) -> PreloadedDataStore:
        schema = self.get_preload_data_params_schema()
        schema.validate_instance(preload_params)
        handlers = []
        for data_id in data_ids:
            if not self.has_data(data_id):
                raise ValueError(f"The requested data_id {data_id} is invalid.")
            handler = ProductHandler.guess(
                data_id,
                self._datasets_info,
                self.cache_store,
                self.credentials,
            )
            if not isinstance(handler, EeaProductHandler):
                raise ValueError(
                    f"The requested data_id {data_id} cannot be "
                    f"preloaded. Try using open_data()"
                )
            handlers.append(handler)

        # Using the first one, because they are all the same if they are all
        # are valid and need to be preloaded.
        return handlers[0].preload_data(*data_ids, **preload_params)

    def get_preload_data_params_schema(self) -> JsonObjectSchema:
        params = dict(
            blocking=JsonBooleanSchema(
                title="Option to make the preload_data method blocking or "
                "non-blocking",
                description=(
                    "If True, (the default) if the constructor should wait for"
                    "all preload task to finish before the calling thread"
                ),
                default=True,
            ),
            silent=JsonBooleanSchema(
                title="Silence the output of Preload API",
                description="If True, you don't want any preload state output."
                "               Defaults to `False`",
                default=False,
            ),
            cleanup=JsonBooleanSchema(
                title="Option to cleanup the download directory before and "
                "after the preload job and to cleanup the cache "
                "directory when preload_handle.close() is called. "
                "Defaults to True.",
                default=True,
            ),
            tile_size=JsonComplexSchema(
                title="Tile size of the final data cube to be saved.",
                default=2000,
                one_of=[
                    JsonIntegerSchema(minimum=1),
                    JsonArraySchema(
                        items=[
                            JsonIntegerSchema(minimum=1),
                            JsonIntegerSchema(minimum=1),
                        ]
                    ),
                ],
            ),
        )
        return JsonObjectSchema(
            properties=dict(**params),
            required=[],
            additional_properties=False,
        )
