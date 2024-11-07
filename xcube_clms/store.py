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

import logging
from abc import ABC
from typing import Tuple, Iterator, Container, Any, Union

import xarray as xr
from xcube.core.store import DataDescriptor, DataStore, DataTypeLike, \
    DATASET_TYPE
from xcube.util.jsonschema import (
    JsonObjectSchema,
    JsonStringSchema,
)

from .clms import CLMS
from .utils import assert_valid_data_type


class CLMSDataStore(DataStore, ABC):
    """CLMS implementation of the data store defined in the ``xcube_clms``
    plugin."""

    def __init__(self, **clms_kwargs):
        self.clms = CLMS(**clms_kwargs)
        logging.basicConfig(level=logging.INFO)

    @classmethod
    def get_data_store_params_schema(cls) -> JsonObjectSchema:
        params = dict(
            url=JsonStringSchema(
                title="URL of CLMS API",
            )
        )
        return JsonObjectSchema(
            properties=dict(**params), required=None, additional_properties=False
        )

    @classmethod
    def get_data_types(cls) -> Tuple[str, ...]:
        return (DATASET_TYPE.alias,)

    def get_data_types_for_data(self, data_id: str) -> Tuple[str, ...]:
        return self.get_data_types()

    def get_data_ids(
        self, data_type: DataTypeLike = None, include_attrs: Container[str] = None
    ) -> Union[Iterator[str], Iterator[tuple[str, dict[str, Any]]]]:
        assert_valid_data_type(data_type)
        data_ids = self.clms.get_data_ids()
        for data_id in data_ids:
            if include_attrs is None:
                yield data_id
            else:
                yield self.clms.get_data_ids_with_attrs(include_attrs, data_id)

    def has_data(self, data_id: str, data_type: DataTypeLike = None) -> bool:
        return self.clms.has_data(data_id, data_type)

    def describe_data(
        self, data_id: str, data_type: DataTypeLike = None
    ) -> DataDescriptor:
        raise NotImplementedError

    def get_data_opener_ids(
        self, data_id: str = None, data_type: DataTypeLike = None
    ) -> Tuple[str, ...]:
        raise NotImplementedError

    def get_open_data_params_schema(
        self, data_id: str = None, opener_id: str = None
    ) -> JsonObjectSchema:
        raise NotImplementedError

    def open_data(
        self, data_id: str, opener_id: str = None, **open_params
    ) -> xr.Dataset:
        return self.clms.open_dataset(data_id, **open_params)

    def search_data(
        self, data_type: DataTypeLike = None, **search_params
    ) -> Iterator[DataDescriptor]:
        raise NotImplementedError("search_data() operation is not supported yet")

    @classmethod
    def get_search_params_schema(
        cls, data_type: DataTypeLike = None
    ) -> JsonObjectSchema:
        pass
