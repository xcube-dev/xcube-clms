# The MIT License (MIT)
# Copyright (c) 2024 by the xcube development team and contributors
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
from xcube.core.store import (
    DataDescriptor,
    DataStore,
    DataTypeLike,
    DATASET_TYPE,
    DatasetDescriptor,
)
from xcube.util.jsonschema import (
    JsonObjectSchema,
    JsonStringSchema,
)

from .clms import CLMS
from .constants import DATA_OPENER_IDS
from .utils import assert_valid_data_type


class CLMSDataStore(DataStore, ABC):
    """CLMS implementation of the data store defined in the ``xcube_clms``
    plugin."""

    def __init__(self, **clms_kwargs):
        self._clms = CLMS(**clms_kwargs)

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
            url=JsonStringSchema(
                title="URL of CLMS API",
            ),
            credentials=JsonObjectSchema(
                dict(**credentials_params),
                required=("client_id", "user_id", "token_uri", "private_key"),
            ),
        )
        return JsonObjectSchema(
            properties=dict(**params),
            required=("url",),
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
        metadata = self._clms.get_extent(data_id)
        return DatasetDescriptor(data_id, **metadata)

    def get_data_opener_ids(
        self, data_id: str = None, data_type: DataTypeLike = None
    ) -> Tuple[str, ...]:
        return DATA_OPENER_IDS

    def get_open_data_params_schema(
        self, data_id: str = None, opener_id: str = None
    ) -> JsonObjectSchema:
        # We do not support any open_data_params yet
        return JsonObjectSchema()

    def open_data(
        self,
        data_id: str,
        opener_id: str = None,
        spatial_coverage: str = "",
        resolution: str = "",
        **open_params,
    ) -> xr.Dataset:
        raise NotImplementedError()

    def search_data(
        self, data_type: DataTypeLike = None, **search_params
    ) -> Iterator[DataDescriptor]:
        raise NotImplementedError("search_data() operation is not supported yet")

    @classmethod
    def get_search_params_schema(
        cls, data_type: DataTypeLike = None
    ) -> JsonObjectSchema:
        pass

    def preload_data(self, *data_ids: str, **preload_params):
        return self._clms.preload_data(data_ids, **preload_params)

    @classmethod
    def get_preload_params_schema(cls) -> JsonObjectSchema:
        return JsonObjectSchema(additional_properties=True)
