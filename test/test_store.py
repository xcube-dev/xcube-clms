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
import unittest
from unittest.mock import patch, MagicMock

import numpy as np
import pytest
import xarray as xr
from xcube.core.store import new_data_store, DatasetDescriptor
from xcube.util.jsonschema import JsonObjectSchema

from xcube_clms.constants import DATA_STORE_ID, DATA_ID_SEPARATOR, \
    DATA_OPENER_IDS
from xcube_clms.preload import ClmsPreloadHandle


class ClmsDataStoreTest(unittest.TestCase):
    def setUp(self):
        self.url = "https://land.copernicus.eu/api"
        self.mock_credentials = {
            "client_id": "",
            "ip_range": None,
            "issued": "",
            "key_id": "c",
            "private_key": "",
            "title": "",
            "token_uri": "",
            "user_id": "",
        }
        self.data_id = "forest-type-2018|FTY_2018_010m_al_03035_v010"
        self.store = new_data_store(
            DATA_STORE_ID,
            credentials=self.mock_credentials,
            cache_store_params={"root": "preload_clms_cache"},
        )

    def tearDown(self):
        patch.stopall()

    @pytest.mark.vcr
    def test_get_data_types(self):
        data_types = self.store.get_data_types()

        expected_data_types = ("dataset",)
        self.assertEqual(data_types, expected_data_types)

    @pytest.mark.vcr()
    def test_list_data_ids(self):
        data_ids = self.store.list_data_ids()

        self.assertIsNot(data_ids, [])
        for data_id in data_ids:
            self.assertEqual(len(data_id.split(DATA_ID_SEPARATOR)), 2)

    @pytest.mark.vcr()
    def test_describe_data(self):
        descriptor = self.store.describe_data(self.data_id)
        expected_descriptor = {
            "data_id": self.data_id,
            "data_type": "dataset",
            "crs": "EPSG:3035",
            "time_range": ("2018-03-01", "2018-10-31"),
        }
        self.assertIsInstance(descriptor, DatasetDescriptor)
        self.assertDictEqual(descriptor.to_dict(), expected_descriptor)

    @pytest.mark.vcr()
    def test_get_data_types_for_data(self):
        data_types = self.store.get_data_types_for_data(self.data_id)
        expected_data_types = ("dataset",)
        self.assertEqual(data_types, expected_data_types)

    @pytest.mark.vcr()
    def test_get_get_data_store_params_schema(self):
        data_store_params_schem = self.store.get_data_store_params_schema()
        self.assertIsInstance(data_store_params_schem, JsonObjectSchema)
        self.assertIn("credentials", data_store_params_schem.properties)
        self.assertIn("cache_store_params", data_store_params_schem.properties)

        data_store_params_schem = self.store.get_data_store_params_schema()
        self.assertIsInstance(data_store_params_schem, JsonObjectSchema)
        self.assertIn("credentials", data_store_params_schem.properties)

    @pytest.mark.vcr()
    def test_get_data_opener_ids(self):
        opener_ids = self.store.get_data_opener_ids()
        expected_opener_ids = DATA_OPENER_IDS
        self.assertEqual(expected_opener_ids, opener_ids)

        opener_ids = self.store.get_data_opener_ids(self.data_id)
        expected_opener_ids = DATA_OPENER_IDS
        self.assertEqual(expected_opener_ids, opener_ids)

    @pytest.mark.vcr()
    def test_has_data(self):
        has_data = self.store.has_data(self.data_id)
        self.assertTrue(has_data)

        not_has_data = self.store.has_data("invalid|data")
        self.assertFalse(not_has_data)

    @pytest.mark.vcr()
    def test_get_open_data_params_schema(self):
        schema = self.store.get_open_data_params_schema(self.data_id)
        self.assertIsInstance(schema, JsonObjectSchema)
        self.assertEqual({}, schema.properties)

    @pytest.mark.vcr()
    def test_search_data(self):
        self.assertRaises(NotImplementedError, self.store.search_data)

    @pytest.mark.vcr()
    def test_get_search_params_schema(self):
        schema = self.store.get_search_params_schema(self.data_id)
        self.assertIsInstance(schema, JsonObjectSchema)
        self.assertEqual({}, schema.properties)

    @pytest.mark.vcr()
    def test_get_preload_data_params_schema(self):
        schema = self.store.get_preload_data_params_schema()
        self.assertIsInstance(schema, JsonObjectSchema)
        self.assertIn("blocking", schema.properties)
        self.assertIn("silent", schema.properties)

    @pytest.mark.vcr()
    @patch("xcube_clms.clms.new_fs_data_store")
    def test_open_data(self, mock_new_data_store):
        mock_fs_data_store = MagicMock()
        mock_fs_data_store.has_data.return_value = True
        mock_dataset = xr.Dataset(
            {
                "temperature": (("time", "x", "y"), np.random.rand(5, 5, 5)),
                "precipitation": (("time", "x", "y"), np.random.rand(5, 5, 5)),
            }
        )
        mock_fs_data_store.open_data.return_value = mock_dataset
        mock_new_data_store.return_value = mock_fs_data_store
        store = new_data_store(
            DATA_STORE_ID,
            credentials=self.mock_credentials,
            cache_store_params={"root": "preload_clms_cache"},
        )
        dataset = store.open_data(self.data_id)

        self.assertIsInstance(dataset, xr.Dataset)
        self.assertCountEqual(["temperature", "precipitation"], list(dataset.data_vars))
        self.assertEqual(dataset["temperature"].shape, (5, 5, 5))
        self.assertEqual(dataset["precipitation"].shape, (5, 5, 5))
        mock_new_data_store.assert_called_once_with("file", root="preload_clms_cache")
        mock_fs_data_store.open_data.assert_called_once_with(
            data_id="forest-type-2018|FTY_2018_010m_al_03035_v010"
        )

    @pytest.mark.vcr()
    @patch("xcube_clms.clms.Clms._access_item")
    @patch("xcube_clms.preload.ClmsApiTokenHandler")
    def test_preload_data(self, mock_token_handler, mock_access_item):
        mock_token_handler.api_token = "mock_token"
        mock_access_item.side_effect = [{"id": "data_id"}, {"id": "product_id"}]

        handle = self.store.preload_data(self.data_id)
        self.assertIsInstance(handle, ClmsPreloadHandle)
        self.assertEqual(
            {
                "forest-type-2018|FTY_2018_010m_al_03035_v010": {
                    "item": {"id": "data_id"},
                    "product": {"id": "product_id"},
                }
            },
            handle.data_id_maps,
        )
