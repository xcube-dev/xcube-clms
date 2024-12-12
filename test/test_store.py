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

import pytest
from xcube.core.store import new_data_store, DatasetDescriptor
from xcube.util.jsonschema import JsonObjectSchema

from xcube_clms.constants import DATA_STORE_ID, DATA_ID_SEPARATOR, DATA_OPENER_IDS


class CLMSTest(unittest.TestCase):
    def setUp(self):
        self.url = "https://land.copernicus.eu/api"
        self.credentials = {
            "client_id": "client_id_value",
            "ip_range": None,
            "issued": "2024-10-30T14:47:22.823084",
            "key_id": "key_id_value",
            "private_key": "private_key_value",
            "title": "clms-test",
            "token_uri": "https://land.copernicus.eu/@@oauth2-token",
            "user_id": "user_id_value",
        }
        self.data_id = "forest-type-2018|FTY_2018_010m_al_03035_v010"

    @pytest.mark.vcr()
    def test_get_data_types(self):
        store = new_data_store(
            DATA_STORE_ID, url=self.url, credentials=self.credentials
        )
        data_types = store.get_data_types()

        expected_data_types = ("dataset",)
        self.assertEqual(data_types, expected_data_types)

    @pytest.mark.vcr()
    def test_list_data_ids(self):
        store = new_data_store(
            DATA_STORE_ID, url=self.url, credentials=self.credentials
        )
        data_ids = store.list_data_ids()

        self.assertIsNot(data_ids, [])
        for data_id in data_ids:
            self.assertEqual(len(data_id.split(DATA_ID_SEPARATOR)), 2)

    @pytest.mark.vcr()
    def test_describe_data(self):
        store = new_data_store(
            DATA_STORE_ID, url=self.url, credentials=self.credentials
        )
        descriptor = store.describe_data(self.data_id)
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
        store = new_data_store(
            DATA_STORE_ID, url=self.url, credentials=self.credentials
        )
        data_types = store.get_data_types_for_data(self.data_id)
        expected_data_types = ("dataset",)
        self.assertEqual(data_types, expected_data_types)

    @pytest.mark.vcr()
    def test_get_get_data_store_params_schema(self):
        store = new_data_store(
            DATA_STORE_ID, url=self.url, credentials=self.credentials
        )
        data_store_params_schem = store.get_data_store_params_schema()
        self.assertIsInstance(data_store_params_schem, JsonObjectSchema)
        self.assertIn("url", data_store_params_schem.properties)
        self.assertIn("credentials", data_store_params_schem.properties)
        self.assertIn("path", data_store_params_schem.properties)

    @pytest.mark.vcr()
    def test_get_data_opener_ids(self):
        store = new_data_store(
            DATA_STORE_ID, url=self.url, credentials=self.credentials
        )
        opener_ids = store.get_data_opener_ids()
        expected_opener_ids = DATA_OPENER_IDS
        self.assertEqual(opener_ids, expected_opener_ids)

        opener_ids = store.get_data_opener_ids(self.data_id)
        expected_opener_ids = DATA_OPENER_IDS
        self.assertEqual(opener_ids, expected_opener_ids)
