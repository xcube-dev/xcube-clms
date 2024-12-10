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
import unittest
from unittest.mock import patch, MagicMock

import numpy as np
import pytest
import xarray as xr

from xcube_clms.clms import CLMS
from xcube_clms.constants import DATA_ID_SEPARATOR


class TestCLMS(unittest.TestCase):
    def __init__(self, methodName: str = "runTest"):
        super().__init__(methodName)
        self.mock_dataset = xr.Dataset(
            {"temperature": (("time", "x", "y"), np.random.rand(5, 5, 5))}
        )

    def setUp(self):
        self.mock_url = "http://mock-api.com"
        self.mock_credentials = {
            "client_id": "test_client_id",
            "user_id": "test_user_id",
            "token_uri": "test_token_uri",
            "private_key": "test_private_key",
        }
        self.mock_path = "mockpath"

    @patch("xcube_clms.clms.CLMS._fetch_all_datasets")
    @patch("xcube_clms.preload.CLMSAPITokenHandler")
    @patch("xcube_clms.preload.PreloadData")
    def test_initialization(
        self,
        mock_preload,
        mock_clms_api_token,
        mock_fetch_datasets,
    ):

        mock_token_instance = MagicMock()
        mock_token_instance.access_token = "mocked_access_token"
        mock_clms_api_token.return_value = mock_token_instance

        mock_preload_instance = MagicMock()
        mock_preload.return_value = mock_preload_instance
        mock_preload_instance._clms_api_token_instance = mock_token_instance
        mock_preload_instance._api_token = "mocked_access_token"

        mock_fetch_datasets.return_value = [
            {"dataset_id": "1", "name": "dataset1", "type": "type1"},
            {"dataset_id": "2", "name": "dataset2", "type": "type2"},
        ]

        clms = CLMS(self.mock_url, self.mock_credentials, self.mock_path)

        mock_clms_api_token.assert_called_once_with(self.mock_credentials)
        self.assertEqual(clms._url, self.mock_url)
        self.assertEqual(clms.path, os.path.join(os.getcwd(), self.mock_path))
        mock_fetch_datasets.assert_called_once()
        self.assertEqual(
            clms._datasets_info,
            [
                {"dataset_id": "1", "name": "dataset1", "type": "type1"},
                {"dataset_id": "2", "name": "dataset2", "type": "type2"},
            ],
        )

    @patch("xcube_clms.clms.os.listdir")
    @patch("xcube_clms.clms.CLMS._fetch_all_datasets")
    @patch("xcube_clms.preload.CLMSAPITokenHandler")
    @patch("xcube_clms.preload.PreloadData")
    def test_open_data(
        self,
        mock_preload,
        mock_clms_api_token,
        mock_fetch_datasets,
        mock_os_listdir,
    ):
        data_id = "product_id|file_id"

        mock_token_instance = MagicMock()
        mock_token_instance.access_token = "mocked_access_token"
        mock_clms_api_token.return_value = mock_token_instance

        mock_preload_instance = MagicMock()
        mock_preload_instance._clms_api_token_instance = mock_token_instance
        mock_preload_instance._api_token = "mocked_access_token"
        mock_preload_instance.view_cache.return_value = {data_id: "file_id/"}
        mock_preload.return_value = mock_preload_instance

        mock_os_listdir.return_value = ["file_id/"]

        mock_fetch_datasets.return_value = [
            {"dataset_id": "1", "name": "dataset1", "type": "type1"},
            {"dataset_id": "2", "name": "dataset2", "type": "type2"},
        ]

        # mock_file_store.return_value.open_data.return_value = self.mock_dataset
        clms = CLMS(self.mock_url, self.mock_credentials, self.mock_path)
        clms._preload_data = mock_preload_instance

        mock_file_store = MagicMock()
        mock_file_store.open_data.return_value = self.mock_dataset
        clms.file_store = mock_file_store

        opened_data = clms.open_data(data_id)
        self.assertIsInstance(opened_data, xr.Dataset)

        clms = CLMS(self.mock_url, self.mock_credentials, self.mock_path)
        mock_preload_instance.view_cache.return_value = {}
        clms._preload_data = mock_preload_instance

        with pytest.raises(
            FileNotFoundError, match=f"No cached data found for data_id: {data_id}"
        ):
            clms.open_data(data_id)

        data_id = "invalid_data_id"
        with pytest.raises(
            ValueError,
            match="Expected a data_id in the format {{product_id}}"
            f"{DATA_ID_SEPARATOR}{{file_id}} but got {data_id}",
        ):
            clms.open_data(data_id)

    @patch("xcube_clms.clms.CLMS._fetch_all_datasets")
    @patch("xcube_clms.preload.CLMSAPITokenHandler")
    @patch("xcube_clms.preload.PreloadData")
    def test_get_data_ids(
        self,
        mock_preload,
        mock_clms_api_token,
        mock_fetch_datasets,
    ):
        mock_token_instance = MagicMock()
        mock_token_instance.access_token = "mocked_access_token"
        mock_clms_api_token.return_value = mock_token_instance

        mock_preload_instance = MagicMock()
        mock_preload_instance._clms_api_token_instance = mock_token_instance
        mock_preload_instance._api_token = "mocked_access_token"
        mock_preload_instance.cache = {
            "data_1|file1": "file_1",
            "data_2|file2": "file_2",
        }
        mock_preload.return_value = mock_preload_instance

        mock_fetch_datasets.return_value = [
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

        clms = CLMS(self.mock_url, self.mock_credentials, self.mock_path)
        clms._preload_data = mock_preload_instance
        data_ids = list(clms.get_data_ids())
        self.assertEqual(data_ids, ["dataset1|file1", "dataset2|file2"])

        result = list(clms.get_data_ids(include_attrs=True))
        assert result == [
            (
                "dataset1|file1",
                {"area": "area1", "file": "file1", "format": "geotiff"},
            ),
            (
                "dataset2|file2",
                {"area": "area2", "file": "file2", "format": "geotiff"},
            ),
        ]

        result = list(clms.get_data_ids(include_attrs=["area"]))
        assert result == [
            ("dataset1|file1", {"area": "area1"}),
            ("dataset2|file2", {"area": "area2"}),
        ]

    @patch("xcube_clms.clms.CLMS._fetch_all_datasets")
    @patch("xcube_clms.preload.CLMSAPITokenHandler")
    @patch("xcube_clms.preload.PreloadData")
    @patch("xcube_clms.clms.is_valid_data_type")
    @patch("xcube_clms.clms.CLMS._get_item")
    def test_has_data(
        self,
        mock_get_item,
        mock_is_valid_data_type,
        mock_preload,
        mock_clms_api_token,
        mock_fetch_datasets,
    ):
        mock_token_instance = MagicMock()
        mock_token_instance.access_token = "mocked_access_token"
        mock_clms_api_token.return_value = mock_token_instance

        mock_preload_instance = MagicMock()
        mock_preload_instance._clms_api_token_instance = mock_token_instance
        mock_preload_instance._api_token = "mocked_access_token"
        mock_preload_instance.cache = {
            "dataset1|file1": "file_1",
            "dataset2|file2": "file_2",
        }
        mock_preload.return_value = mock_preload_instance

        mock_fetch_datasets.return_value = [
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

        clms = CLMS(self.mock_url, self.mock_credentials, self.mock_path)

        # Case 1: Valid data type and dataset exists
        mock_is_valid_data_type.return_value = True
        mock_get_item.return_value = {"some": "data"}
        assert clms.has_data("valid_id", "valid_type") is True
        mock_is_valid_data_type.assert_called_once_with("valid_type")
        mock_get_item.assert_called_once_with("valid_id")

        mock_is_valid_data_type.reset_mock()
        mock_get_item.reset_mock()

        # Case 2: Valid data type but dataset does not exist
        mock_is_valid_data_type.return_value = True
        mock_get_item.return_value = None
        assert clms.has_data("invalid_id", "valid_type") is False
        mock_is_valid_data_type.assert_called_once_with("valid_type")
        mock_get_item.assert_called_once_with("invalid_id")

        mock_is_valid_data_type.reset_mock()
        mock_get_item.reset_mock()

        # Case 3: Invalid data type
        mock_is_valid_data_type.return_value = False
        assert clms.has_data("valid_id", "invalid_type") is False
        mock_is_valid_data_type.assert_called_once_with("invalid_type")
        mock_get_item.assert_not_called()

    @patch("xcube_clms.clms.CLMS._access_item")
    @patch("xcube_clms.clms.CLMS._fetch_all_datasets")
    @patch("xcube_clms.preload.CLMSAPITokenHandler")
    @patch("xcube_clms.preload.PreloadData")
    def test_get_extent(
        self, mock_preload, mock_clms_api_token, mock_fetch_datasets, mock_access_item
    ):
        mock_token_instance = MagicMock()
        mock_token_instance.access_token = "mocked_access_token"
        mock_clms_api_token.return_value = mock_token_instance

        mock_preload_instance = MagicMock()
        mock_preload_instance._clms_api_token_instance = mock_token_instance
        mock_preload_instance._api_token = "mocked_access_token"
        mock_preload_instance.cache = {
            "dataset1|file1": "file_1",
            "dataset2|file2": "file_2",
        }
        mock_preload.return_value = mock_preload_instance

        mock_fetch_datasets.return_value = [
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

        mock_access_item.return_value = {
            "file": "file1",
            "area": "area1",
            "format": "geotiff",
        }

        clms = CLMS(self.mock_url, self.mock_credentials, self.mock_path)

        assert (clms.get_extent("dataset1|file1")) == {
            "time_range": (None, None),
            "crs": None,
        }

        mock_access_item.return_value = {
            "file": "file1",
            "coordinateReferenceSystemList": ["WGS84"],
            "temporalExtentStart": "01-12-2022",
            "temporalExtentEnd": "01-12-2024",
        }
        clms = CLMS(self.mock_url, self.mock_credentials, self.mock_path)

        assert (clms.get_extent("dataset1|file1")) == {
            "time_range": ("01-12-2022", "01-12-2024"),
            "crs": "WGS84",
        }

    @patch("xcube_clms.clms.CLMS._fetch_all_datasets")
    @patch("xcube_clms.preload.CLMSAPITokenHandler")
    @patch("xcube_clms.preload.PreloadData")
    def test_create_data_ids(
        self,
        mock_preload,
        mock_clms_api_token,
        mock_fetch_datasets,
    ):
        mock_token_instance = MagicMock()
        mock_token_instance.access_token = "mocked_access_token"
        mock_clms_api_token.return_value = mock_token_instance

        mock_preload_instance = MagicMock()
        mock_preload_instance._clms_api_token_instance = mock_token_instance
        mock_preload_instance._api_token = "mocked_access_token"
        mock_preload_instance.cache = {
            "dataset1|file1": "file_1",
            "dataset2|file2": "file_2",
        }
        mock_preload.return_value = mock_preload_instance

        mock_dataset = [
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

        mock_fetch_datasets.return_value = mock_dataset

        clms = CLMS(self.mock_url, self.mock_credentials, self.mock_path)
        clms._datasets_info = mock_dataset

        result = list(clms._create_data_ids(include_attrs=None))
        expected = [
            "dataset1|file1",
            "dataset2|file2",
        ]
        assert result == expected

        result = list(clms._create_data_ids(include_attrs=True))
        assert result == [
            (
                "dataset1|file1",
                {"area": "area1", "file": "file1", "format": "geotiff"},
            ),
            (
                "dataset2|file2",
                {"area": "area2", "file": "file2", "format": "geotiff"},
            ),
        ]

        result = list(clms._create_data_ids(include_attrs=["area"]))
        assert result == [
            ("dataset1|file1", {"area": "area1"}),
            ("dataset2|file2", {"area": "area2"}),
        ]

        mock_datasets_info = []
        clms._datasets_info = mock_datasets_info

        result = list(clms._create_data_ids(include_attrs=None))
        assert result == []

        result = list(clms._create_data_ids(include_attrs=True))
        assert result == []

        result = list(clms._create_data_ids(include_attrs=["size"]))
        assert result == []

    @patch("xcube_clms.clms.make_api_request")
    @patch("xcube_clms.clms.get_response_of_type")
    def test_fetch_all_datasets(
        self,
        mock_get_response_of_type,
        mock_make_api_request,
    ):

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

        mock_make_api_request.side_effect = [
            first_page_response,
            second_page_response,
            first_page_response,
            second_page_response,
        ]
        mock_get_response_of_type.side_effect = [
            first_page_response,
            second_page_response,
            first_page_response,
            second_page_response,
        ]

        datasets_info = CLMS._fetch_all_datasets("http://mock_page")

        expected_datasets_info = [
            {"dataset_id": "1", "name": "dataset1"},
            {"dataset_id": "2", "name": "dataset2"},
            {"dataset_id": "3", "name": "dataset3"},
        ]

        assert mock_make_api_request.call_count == 2
        assert mock_get_response_of_type.call_count == 2
        assert datasets_info == expected_datasets_info

    @patch("xcube_clms.clms.CLMS._get_item")
    @patch("xcube_clms.clms.CLMS._fetch_all_datasets")
    @patch("xcube_clms.preload.CLMSAPITokenHandler")
    @patch("xcube_clms.preload.PreloadData")
    def test_access_item(
        self, mock_preload, mock_clms_api_token, mock_fetch_datasets, mock_get_item
    ):
        mock_token_instance = MagicMock()
        mock_token_instance.access_token = "mocked_access_token"
        mock_clms_api_token.return_value = mock_token_instance

        mock_preload_instance = MagicMock()
        mock_preload_instance._clms_api_token_instance = mock_token_instance
        mock_preload_instance._api_token = "mocked_access_token"
        mock_preload_instance.cache = {
            "dataset1|file1": "file_1",
            "dataset2|file2": "file_2",
        }
        mock_preload.return_value = mock_preload_instance

        mock_dataset = [
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

        mock_fetch_datasets.return_value = mock_dataset

        mock_get_item.return_value = [
            {
                "file": "file2",
                "area": "area2",
                "format": "geotiff",
            }
        ]

        clms = CLMS(self.mock_url, self.mock_credentials, self.mock_path)
        item = clms._access_item("dataset2|file2")
        expected_item = {"file": "file2", "area": "area2", "format": "geotiff"}
        assert item == expected_item

    @patch("xcube_clms.clms.CLMS._fetch_all_datasets")
    @patch("xcube_clms.preload.CLMSAPITokenHandler")
    @patch("xcube_clms.preload.PreloadData")
    def test_get_item(
        self,
        mock_preload,
        mock_clms_api_token,
        mock_fetch_datasets,
    ):
        mock_token_instance = MagicMock()
        mock_token_instance.access_token = "mocked_access_token"
        mock_clms_api_token.return_value = mock_token_instance

        mock_preload_instance = MagicMock()
        mock_preload_instance._clms_api_token_instance = mock_token_instance
        mock_preload_instance._api_token = "mocked_access_token"
        mock_preload_instance.cache = {
            "dataset1|file1": "file_1",
            "dataset2|file2": "file_2",
        }
        mock_preload.return_value = mock_preload_instance

        mock_dataset = [
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

        mock_fetch_datasets.return_value = mock_dataset

        clms = CLMS(self.mock_url, self.mock_credentials, self.mock_path)
        item = clms._get_item("dataset2|file2")
        expected_item = [{"file": "file2", "area": "area2", "format": "geotiff"}]
        assert item == expected_item

        item = clms._get_item("dataset2")
        expected_item = [
            {
                "id": "dataset2",
                "downloadable_files": {
                    "items": [{"file": "file2", "area": "area2", "format": "geotiff"}]
                },
            }
        ]
        assert item == expected_item
