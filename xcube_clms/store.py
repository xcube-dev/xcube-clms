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
from abc import ABC
from typing import Tuple, Iterator, Container, Any, Union

import xarray as xr
from xcube.core.store import DATASET_TYPE
from xcube.core.store import DataDescriptor
from xcube.core.store import DataStore
from xcube.core.store import DataTypeLike
from xcube.core.store import DatasetDescriptor
from xcube.core.store import MutableDataStore
from xcube.core.store import PreloadHandle
from xcube.util.jsonschema import JsonBooleanSchema
from xcube.util.jsonschema import JsonObjectSchema
from xcube.util.jsonschema import JsonStringSchema

from .clms import Clms
from .constants import DEFAULT_PRELOAD_CACHE_FOLDER
from .utils import assert_valid_data_type


class ClmsDataStore(DataStore, ABC):
    """CLMS implementation of the data store defined in the ``xcube_clms``
    plugin."""

    def __init__(self, **clms_kwargs):
        self._clms = Clms(**clms_kwargs)
        self.cache_store: MutableDataStore = self._clms.cache_store
        self.data_opener_id = f"dataset:zarr:{self.cache_store.protocol}"

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
                "the steps outlined here. https://eea.github.io/clms-api-docs/authentication.html",
                required=("client_id", "user_id", "token_uri", "private_key"),
            ),
            cache_store_id=JsonStringSchema(
                title="Store ID of cache data store.",
                description=(
                    "Store ID of a filesystem-based data store implemented in xcube."
                ),
                default="file",
            ),
            cache_store_params=JsonObjectSchema(
                title="Store parameters of cache data store.",
                description=(
                    "Parameters of a filesystem-based data store implemented in xcube. "
                    "Provide parameters for a file data store if `cache_store_id` is not provided."
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
        include_attrs: Container[str] | bool | None = None,
    ) -> Union[Iterator[str], Iterator[tuple[str, dict[str, Any]]]]:
        assert_valid_data_type(data_type)
        data_ids = self._clms.get_data_ids(include_attrs)
        for data_id in data_ids:
            if ((include_attrs is not None) and (include_attrs != False)) or (
                include_attrs == True
            ):
                yield data_id[0], data_id[1]
            else:
                yield data_id

    def has_data(self, data_id: str, data_type: DataTypeLike = None) -> bool:
        return self._clms.has_data(data_id, data_type)

    def describe_data(
        self, data_id: str, data_type: DataTypeLike = None
    ) -> DataDescriptor:
        assert_valid_data_type(data_type)
        metadata = self._clms.describe_data(data_id)
        return DatasetDescriptor(data_id, **metadata)

    def get_data_opener_ids(
        self, data_id: str = None, data_type: DataTypeLike = None
    ) -> Tuple[str, ...]:
        return (self.data_opener_id,)

    def get_open_data_params_schema(
        self, data_id: str = None, opener_id: str = None
    ) -> JsonObjectSchema:
        if opener_id is None:
            opener_id = self.data_opener_id
        return self.cache_store.get_open_data_params_schema(data_id, opener_id)

    def open_data(
        self,
        data_id: str,
        opener_id: str = None,
        **open_params,
    ) -> xr.Dataset:
        return self._clms.open_data(data_id, opener_id, **open_params)

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
    ) -> PreloadHandle:
        schema = self.get_preload_data_params_schema()
        schema.validate_instance(preload_params)
        return self._clms.preload_data(*data_ids, **preload_params)

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
                title="Option to cleanup the directory in case there were "
                "multiple files downloaded for the same data_id. "
                "Defaults to True.",
            ),
        )
        return JsonObjectSchema(
            properties=dict(**params),
            required=[],
            additional_properties=False,
        )
