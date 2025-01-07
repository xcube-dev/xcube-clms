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
import xarray as xr

from xcube_clms.clms import Clms


class ClmsTest(unittest.TestCase):
    def __init__(self, methodName: str = "runTest"):
        super().__init__(methodName)
        self.mock_dataset = xr.Dataset(
            {"temperature": (("time", "x", "y"), np.random.rand(5, 5, 5))}
        )

    def setUp(self):
        self.test_path = "/mock/path"
        self.cache_data_params = {"root": self.test_path}
        self.mock_file_store = MagicMock()
        self.mock_credentials = {
            "client_id": "test_client_id",
            "user_id": "test_user_id",
            "token_uri": "test_token_uri",
            "private_key": "test_private_key",
        }
        self.data_id = "product_id|file_id"

        self.datasets_info = [
            {
                "id": "dataset1",
                "downloadable_files": {
                    "items": [{"file": "file1", "area": "area1", "format": "geotiff"}]
                },
            },
            {
                "id": "dataset2",
                "downloadable_files": {
                    "items": [{"file": "file2", "area": "area2", "format": "geotiff"}]
                },
            },
        ]
        self.mock_make_api_request_patcher = patch("xcube_clms.clms.make_api_request")
        self.mock_get_response_of_type_patcher = patch(
            "xcube_clms.clms.get_response_of_type"
        )
        self.mock_is_valid_data_type_patcher = patch(
            "xcube_clms.clms.is_valid_data_type"
        )

        self.mock_make_api_request = self.mock_make_api_request_patcher.start()
        self.mock_get_response_of_type = self.mock_get_response_of_type_patcher.start()
        self.mock_is_valid_data_type = self.mock_is_valid_data_type_patcher.start()

        self.mock_data_store = MagicMock()
        if self._testMethodName != "test_fetch_all_datasets":
            self.mock_fetch_all_datasets_patcher = patch(
                "xcube_clms.clms.Clms._fetch_all_datasets"
            )
            self.mock_fetch_all_datasets = self.mock_fetch_all_datasets_patcher.start()
            self.mock_fetch_all_datasets.return_value = self.datasets_info

    def tearDown(self):
        patch.stopall()

    def test_initialization(self):
        clms = Clms(self.mock_credentials, cache_store_params=self.cache_data_params)

        self.assertEqual(self.test_path, clms._cache_root)
        self.assertEqual(self.datasets_info, clms._datasets_info)

    @patch("xcube_clms.clms.new_fs_data_store")
    def test_open_data(self, mocked_cache_store):
        mocked_cache_store.return_value.open_data.return_value = self.mock_dataset

        clms = Clms(self.mock_credentials, cache_store_params=self.cache_data_params)

        opened_data = clms.open_data(self.data_id)
        self.assertIsInstance(opened_data, xr.Dataset)

        mocked_cache_store.return_value.has_data.return_value = False
        with self.assertRaises(FileNotFoundError):
            clms.open_data("non-existing|data-id")

        data_id = "invalid_data_id"
        with self.assertRaises(ValueError):
            clms.open_data(data_id)

    def test_get_data_ids(self):
        clms = Clms(self.mock_credentials, cache_store_params=self.cache_data_params)
        data_ids = list(clms.get_data_ids())
        self.assertEqual(data_ids, ["dataset1|file1", "dataset2|file2"])

        result = list(clms.get_data_ids(include_attrs=True))
        self.assertEqual(
            [
                (
                    "dataset1|file1",
                    {"area": "area1", "file": "file1", "format": "geotiff"},
                ),
                (
                    "dataset2|file2",
                    {"area": "area2", "file": "file2", "format": "geotiff"},
                ),
            ],
            result,
        )

        result = list(clms.get_data_ids(include_attrs=["area"]))
        self.assertEqual(
            [
                ("dataset1|file1", {"area": "area1"}),
                ("dataset2|file2", {"area": "area2"}),
            ],
            result,
        )

    @patch("xcube_clms.clms.Clms._get_item")
    def test_has_data(self, mock_get_item):
        clms = Clms(self.mock_credentials, cache_store_params=self.cache_data_params)

        # Case 1: Valid data type and dataset exists
        self.mock_is_valid_data_type.return_value = True
        mock_get_item.return_value = {"some": "data"}
        self.assertEqual(True, clms.has_data("valid_id", "valid_type"))

        # Case 2: Valid data type but dataset does not exist
        self.mock_is_valid_data_type.return_value = True
        mock_get_item.return_value = None
        self.assertEqual(False, clms.has_data("invalid_id", "valid_type"))

        # Case 3: Invalid data type
        self.mock_is_valid_data_type.return_value = False
        self.assertEqual(False, clms.has_data("valid_id", "invalid_type"))

    @patch("xcube_clms.clms.Clms._access_item")
    def test_get_extent(self, mock_access_item):
        mock_access_item.return_value = {
            "file": "file1",
            "area": "area1",
            "format": "geotiff",
        }

        clms = Clms(self.mock_credentials, cache_store_params=self.cache_data_params)

        self.assertEqual(
            {
                "time_range": (None, None),
                "crs": None,
            },
            clms.get_extent("dataset1|file1"),
        )

        mock_access_item.return_value = {
            "file": "file1",
            "coordinateReferenceSystemList": ["WGS84"],
            "temporalExtentStart": "01-12-2022",
            "temporalExtentEnd": "01-12-2024",
        }
        clms = Clms(self.mock_credentials, cache_store_params=self.cache_data_params)

        self.assertEqual(
            {
                "time_range": ("01-12-2022", "01-12-2024"),
                "crs": "WGS84",
            },
            clms.get_extent("dataset1|file1"),
        )

    def test_create_data_ids(self):
        clms = Clms(self.mock_credentials, cache_store_params=self.cache_data_params)
        clms._datasets_info = self.datasets_info

        result = list(clms._create_data_ids(include_attrs=None))
        expected = [
            "dataset1|file1",
            "dataset2|file2",
        ]
        self.assertEqual(expected, result)

        result = list(clms._create_data_ids(include_attrs=True))
        self.assertEqual(
            [
                (
                    "dataset1|file1",
                    {"area": "area1", "file": "file1", "format": "geotiff"},
                ),
                (
                    "dataset2|file2",
                    {"area": "area2", "file": "file2", "format": "geotiff"},
                ),
            ],
            result,
        )

        result = list(clms._create_data_ids(include_attrs=["area"]))
        self.assertEqual(
            [
                ("dataset1|file1", {"area": "area1"}),
                ("dataset2|file2", {"area": "area2"}),
            ],
            result,
        )

        mock_datasets_info = []
        clms._datasets_info = mock_datasets_info

        result = list(clms._create_data_ids(include_attrs=None))
        self.assertEqual([], result)

        result = list(clms._create_data_ids(include_attrs=True))
        self.assertEqual([], result)

        result = list(clms._create_data_ids(include_attrs=["size"]))
        self.assertEqual([], result)

    def test_fetch_all_datasets(self):
        first_page_response = {
            "items": [
                {"dataset_id": "1", "name": "dataset1"},
                {"dataset_id": "2", "name": "dataset2"},
            ],
            "batching": {"next": "http://mock_next_page"},
        }
        second_page_response = {
            "items": [{"dataset_id": "3", "name": "dataset3"}],
            "batching": {},
        }

        self.mock_make_api_request.side_effect = [
            first_page_response,
            second_page_response,
        ]
        self.mock_get_response_of_type.side_effect = [
            first_page_response,
            second_page_response,
        ]
        datasets_info = Clms._fetch_all_datasets()

        expected_datasets_info = [
            {"dataset_id": "1", "name": "dataset1"},
            {"dataset_id": "2", "name": "dataset2"},
            {"dataset_id": "3", "name": "dataset3"},
        ]

        self.assertEqual(expected_datasets_info, datasets_info)

    def test_access_item(self):
        clms = Clms(self.mock_credentials, cache_store_params=self.cache_data_params)
        item = clms._access_item("dataset2|file2")
        expected_item = {"file": "file2", "area": "area2", "format": "geotiff"}
        self.assertEqual(expected_item, item)

    def test_get_item(self):
        clms = Clms(self.mock_credentials, cache_store_params=self.cache_data_params)
        item = clms._get_item("dataset2|file2")
        expected_item = [{"file": "file2", "area": "area2", "format": "geotiff"}]
        self.assertEqual(expected_item, item)

        item = clms._get_item("dataset2")
        expected_item = [
            {
                "id": "dataset2",
                "downloadable_files": {
                    "items": [{"file": "file2", "area": "area2", "format": "geotiff"}]
                },
            }
        ]
        self.assertEqual(expected_item, item)
