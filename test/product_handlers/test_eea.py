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

import logging
import os
import unittest
from collections import defaultdict
from datetime import datetime, timedelta
from unittest.mock import MagicMock, Mock, call, patch

import fsspec
import numpy as np
import rasterio
import xarray as xr
from rasterio.transform import from_origin
from xcube.core.store import DataStoreError, PreloadState

from xcube_clms.constants import (
    CLMS_API_URL,
    DATA_ID_SEPARATOR,
    DOWNLOAD_ENDPOINT,
    TASK_STATUS_ENDPOINT,
    TIME_TO_EXPIRE,
)
from xcube_clms.product_handlers.eea import _UNDEFINED, EeaProductHandler, has_expired
from xcube_clms.utils import (
    find_geo_in_dir,
    get_authorization_header,
    get_dataset_download_info,
)

LOG = logging.getLogger(__name__)


class TestEeaProductHandler(unittest.TestCase):
    def setUp(self):
        self.mock_dataset = xr.Dataset(
            {"temperature": (("time", "x", "y"), np.random.rand(5, 5, 5))}
        )
        self.mock_api_token_handler = MagicMock()
        self.mock_api_token_handler.api_token = "mock_token"
        self.test_path = "/tmp/test/path"
        self.mock_cache_store = MagicMock()
        self.mock_cache_store.root = "/tmp"
        self.mock_data_id = "dataset_id|file_id"

        self.mock_datasets_info = [
            {
                "id": "mock_uid_mock_id",
                "UID": "mock_uid",
                "dataset_download_information": {
                    "items": [
                        {"@id": "mock_id", "path": "mock_path", "full_source": "EEA"}
                    ]
                },
            },
            {
                "id": "mock_uid_legacy_mock_id",
                "UID": "mock_uid",
                "dataset_download_information": {
                    "items": [
                        {"@id": "mock_id", "path": "mock_path", "full_source": "LEGACY"}
                    ]
                },
                "characteristics_temporal_extent": "2024-2024",
            },
            {
                "id": "mock_uid_legacy_present_mock_id",
                "UID": "mock_uid",
                "dataset_download_information": {
                    "items": [
                        {"@id": "mock_id", "path": "mock_path", "full_source": "LEGACY"}
                    ]
                },
                "characteristics_temporal_extent": "2024-present",
            },
            {
                "id": "dataset_id",
                "UID": "dataset_id",
                "dataset_download_information": {
                    "items": [
                        {"@id": "file_id", "path": "mock_path", "full_source": "EEA"}
                    ]
                },
                "downloadable_files": {
                    "items": [{"file": "file_id", "@id": "file_@id"}]
                },
            },
        ]

        self.eea_handler = EeaProductHandler(
            cache_store=self.mock_cache_store,
            datasets_info=self.mock_datasets_info,
            api_token_handler=self.mock_api_token_handler,
        )

        mock_make_api_request_patcher = patch(
            "xcube_clms.product_handlers.eea.make_api_request"
        )
        mock_get_response_of_type_patcher = patch(
            "xcube_clms.product_handlers.eea.get_response_of_type"
        )
        mock_download_zip_data_patcher = patch(
            "xcube_clms.product_handlers.eea.download_zip_data"
        )
        mock_has_expired_patcher = patch("xcube_clms.product_handlers.eea.has_expired")

        self.mock_make_api_request = mock_make_api_request_patcher.start()
        self.mock_get_response_of_type = mock_get_response_of_type_patcher.start()
        self.mock_download_zip_data = mock_download_zip_data_patcher.start()
        self.mock_has_expired = mock_has_expired_patcher.start()

    def tearDown(self):
        patch.stopall()

    def test_existing_complete_request(self):
        self.eea_handler._get_current_requests_status = MagicMock()
        self.eea_handler._get_current_requests_status.return_value = (
            "COMPLETE",
            "existing_task_id",
        )

        task_ids = self.eea_handler.request_download("mock_uid_mock_id")
        self.assertEqual(task_ids, ["existing_task_id"])
        self.eea_handler._get_current_requests_status.assert_called_once_with(
            data_id="mock_uid_mock_id"
        )
        self.mock_api_token_handler.refresh_token.assert_called_once()
        self.mock_make_api_request.assert_not_called()

    def test_existing_pending_request(self):
        self.eea_handler._get_current_requests_status = MagicMock()
        self.eea_handler._get_current_requests_status.return_value = (
            "PENDING",
            "existing_task_id",
        )
        task_ids = self.eea_handler.request_download("mock_uid_mock_id")
        self.assertEqual(task_ids, ["existing_task_id"])
        self.eea_handler._get_current_requests_status.assert_called_once_with(
            data_id="mock_uid_mock_id"
        )
        self.mock_api_token_handler.refresh_token.assert_called_once()
        self.mock_make_api_request.assert_not_called()

    def test_new_request(self):
        self.eea_handler._get_current_requests_status = MagicMock()
        self.eea_handler._get_current_requests_status.return_value = (
            _UNDEFINED,
            "",
        )
        response_data = {"TaskIds": [{"TaskID": "mock_task_id"}]}
        self.mock_get_response_of_type.return_value = response_data

        task_ids = self.eea_handler.request_download("mock_uid_mock_id")
        self.assertEqual(task_ids, ["mock_task_id"])
        self.eea_handler._get_current_requests_status.assert_called_once_with(
            data_id="mock_uid_mock_id"
        )
        self.mock_api_token_handler.refresh_token.assert_called_once()
        self.mock_make_api_request.assert_called_once_with(
            method="POST",
            url=f"{CLMS_API_URL}/{DOWNLOAD_ENDPOINT}",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": "Bearer mock_token",
            },
            json={"Datasets": [{"DatasetID": "mock_uid", "FileID": "mock_id"}]},
        )
        self.mock_get_response_of_type.assert_called_once()

    def test_cancelled_request(self):
        self.eea_handler._get_current_requests_status = MagicMock()
        self.eea_handler._get_current_requests_status.return_value = (
            "CANCELLED",
            "cancelled_task_id",
        )
        response_data = {"TaskIds": [{"TaskID": "new_mock_task_id"}]}
        self.mock_get_response_of_type.return_value = response_data

        task_ids = self.eea_handler.request_download("mock_uid_mock_id")
        self.assertEqual(task_ids, ["new_mock_task_id"])
        self.eea_handler._get_current_requests_status.assert_called_once_with(
            data_id="mock_uid_mock_id"
        )
        self.mock_api_token_handler.refresh_token.assert_called_once()
        self.mock_make_api_request.assert_called_once()
        self.mock_get_response_of_type.assert_called_once()

    def test_get_download_url_success(self):
        response = {
            "mock_task_id": {
                "Status": "Finished_ok",
                "DownloadURL": "http://mock-download-url",
                "FileSize": 12345,
            }
        }
        self.mock_get_response_of_type.return_value = response
        task_id = "mock_task_id"

        result = self.eea_handler._get_download_url(task_id)

        self.assertEqual(("http://mock-download-url", 12345), result)
        self.mock_api_token_handler.refresh_token.assert_called_once()
        self.mock_make_api_request.assert_called_once_with(
            url=f"{CLMS_API_URL}/{TASK_STATUS_ENDPOINT}",
            headers={
                "Accept": "application/json",
                "Authorization": "Bearer mock_token",
            },
        )
        self.mock_get_response_of_type.assert_called_once()

    def test_get_download_url_pending_task(self):
        response_data = {"task_id1": {"Status": "In_progress"}}
        self.mock_get_response_of_type.return_value = response_data
        with self.assertRaisesRegex(
            Exception,
            "Task ID task_id1 has not yet finished. No download url available yet.",
        ):
            self.eea_handler._get_download_url("task_id1")
        self.mock_api_token_handler.refresh_token.assert_called_once()
        self.mock_make_api_request.assert_called_once()
        self.mock_get_response_of_type.assert_called_once()

    def test_get_download_url_invalid_response(self):
        response_data = {"invalid_key": "invalid_value"}
        self.mock_get_response_of_type.return_value = response_data
        url, size = self.eea_handler._get_download_url("task_id1")
        self.assertEqual("", url)
        self.assertEqual(-1, size)
        self.mock_api_token_handler.refresh_token.assert_called_once()
        self.mock_make_api_request.assert_called_once()
        self.mock_get_response_of_type.assert_called_once()

    def test_get_download_url_no_download_url_key(self):
        response_data = {"task_id1": {"Status": "Finished_ok", "FileSize": 123}}
        self.mock_get_response_of_type.return_value = response_data
        url, size = self.eea_handler._get_download_url("task_id1")
        self.assertEqual("", url)
        self.assertEqual(-1, size)
        self.mock_api_token_handler.refresh_token.assert_called_once()
        self.mock_make_api_request.assert_called_once()
        self.mock_get_response_of_type.assert_called_once()

    def test_get_current_requests_status_by_task_id(self):
        response_data = {
            "task_id1": {
                "Status": "Finished_ok",
                "Datasets": [{"DatasetID": "dataset_id", "FileID": "file_id"}],
                "FinalizationDateTime": "2025-01-01T12:00:00",
            }
        }
        self.mock_get_response_of_type.return_value = response_data
        self.mock_has_expired.return_value = False
        status, task_id = self.eea_handler._get_current_requests_status(
            task_id="task_id1"
        )
        self.assertEqual("task_id1", task_id)
        self.assertEqual("COMPLETE", status)
        self.mock_api_token_handler.refresh_token.assert_called_once()
        self.mock_make_api_request.assert_called_once()
        self.mock_get_response_of_type.assert_called_once()

    def test_open_data(self):
        self.eea_handler.cache_store.open_data.return_value = self.mock_dataset
        opened_data = self.eea_handler.open_data("product_id|file_id")
        self.assertIsInstance(opened_data, xr.Dataset)

        self.eea_handler.cache_store.has_data.return_value = False
        with self.assertRaises(DataStoreError):
            self.eea_handler.open_data("non-existing|data-id")

    def test_open_data_file_not_found(self):
        self.mock_cache_store.has_data.return_value = False

        with self.assertRaises(DataStoreError):
            self.eea_handler.open_data("data_id")

    @patch("xcube_clms.product_handlers.eea.ClmsPreloadHandle")
    def test_preload_data(self, mock_preload_handle):
        data_id = "dataset_id|file_id"
        self.eea_handler.preload_data(data_id)
        mock_preload_handle.assert_called_with(
            data_id_maps={
                "dataset_id|file_id": {
                    "item": {"file": "file_id", "@id": "file_@id"},
                    "product": {
                        "id": "dataset_id",
                        "UID": "dataset_id",
                        "dataset_download_information": {
                            "items": [
                                {
                                    "@id": "file_id",
                                    "path": "mock_path",
                                    "full_source": "EEA",
                                }
                            ]
                        },
                        "downloadable_files": {
                            "items": [{"file": "file_id", "@id": "file_@id"}]
                        },
                    },
                }
            },
            url="https://land.copernicus.eu/api",
            cache_store=self.mock_cache_store,
            preload_data=self.eea_handler._preload_data,
        )

    def test__preload_data(self):
        with (
            patch.object(
                self.eea_handler, "request_download", return_value=["task_123"]
            ),
            patch.object(
                self.eea_handler,
                "_get_current_requests_status",
                side_effect=[("COMPLETE", {})],
            ),
            patch.object(
                self.eea_handler,
                "_get_download_url",
                return_value=("http://download.zip", {}),
            ),
            patch("xcube_clms.product_handlers.eea.download_zip_data"),
            patch.object(self.eea_handler, "preprocess_data"),
        ):
            handle_mock = MagicMock()
            self.eea_handler._preload_data(handle_mock, "dataset|item")

            expected_calls = [
                call(
                    PreloadState(
                        data_id="dataset|item",
                        progress=0.1,
                        message="Task ID task_123: Download request in queue.",
                    )
                ),
                call(
                    PreloadState(
                        data_id="dataset|item",
                        progress=0.4,
                        message="Task ID task_123: Download link created. "
                        "Downloading and extracting now...",
                    )
                ),
                call(
                    PreloadState(
                        data_id="dataset|item",
                        progress=0.8,
                        message="Task ID task_123: Extraction complete. "
                        "Processing now...",
                    )
                ),
                call(
                    PreloadState(
                        data_id="dataset|item",
                        progress=1.0,
                        message="Task ID task_123: Preloading Complete.",
                    )
                ),
            ]

            for expected_call, actual_call in zip(
                expected_calls, handle_mock.notify.call_args_list
            ):
                expected_args = expected_call.args[0]
                actual_args = actual_call.args[0]

                assert expected_args.data_id == actual_args.data_id
                assert abs(expected_args.progress - actual_args.progress) < 1e-6
                assert expected_args.message == actual_args.message

    def test_product_type(self):
        self.assertEqual("eea", self.eea_handler.product_type())

    def test_get_current_requests_status_with_multiple_same_data_ids(self):
        self.mock_has_expired.side_effect = [
            False,
            False,
            False,
            False,
        ]
        response_data = {
            "task_id1": {
                "Status": "Finished_ok",
                "Datasets": [{"DatasetID": "dataset_id", "FileID": "file_@id"}],
                "FinalizationDateTime": "2024-11-01T09:00:00",
            },
            "task_id2": {
                "Status": "In_progress",
                "Datasets": [{"DatasetID": "dataset_id", "FileID": "file_@id"}],
                "FinalizationDateTime": "2025-01-01T10:00:00",  # Timestamp
                # for pending is not used in sorting
            },
            "task_id3": {
                "Status": "Cancelled",
                "Datasets": [{"DatasetID": "dataset_id", "FileID": "file_@id"}],
                "FinalizationDateTime": "2025-01-01T11:00:00",
            },
            "task_id4": {
                "Status": "Finished_ok",
                "Datasets": [{"DatasetID": "dataset_id", "FileID": "file_@id"}],
                "FinalizationDateTime": "2025-01-01T12:00:00",
            },
        }
        self.mock_get_response_of_type.return_value = response_data

        status, task_id = self.eea_handler._get_current_requests_status(
            data_id="dataset_id|file_id"
        )
        self.assertEqual("task_id4", task_id)
        self.assertEqual("COMPLETE", status)
        self.mock_api_token_handler.refresh_token.assert_called_once()
        self.mock_make_api_request.assert_called_once()
        self.mock_get_response_of_type.assert_called_once()

    def test_get_current_requests_status_with_multiple_same_data_ids_not_complete(self):
        self.mock_has_expired.return_value = False
        response_data = {
            "task_id2": {
                "Status": "In_progress",
                "Datasets": [{"DatasetID": "dataset_id", "FileID": "file_@id"}],
                "FinalizationDateTime": "2025-01-01T12:00:00",
            },
            "task_id3": {
                "Status": "Cancelled",
                "Datasets": [{"DatasetID": "dataset_id", "FileID": "file_@id"}],
                "FinalizationDateTime": "2025-01-01T12:00:00",
            },
            "task_id4": {
                "Status": "Cancelled",
                "Datasets": [{"DatasetID": "dataset_id", "FileID": "file_@id"}],
                "FinalizationDateTime": "2025-01-01T12:00:00",
            },
        }
        self.mock_get_response_of_type.return_value = response_data

        status, task_id = self.eea_handler._get_current_requests_status(
            data_id="dataset_id|file_id"
        )
        self.assertEqual("PENDING", status)
        self.assertEqual("task_id2", task_id)
        self.mock_api_token_handler.refresh_token.assert_called_once()
        self.mock_make_api_request.assert_called_once()
        self.mock_get_response_of_type.assert_called_once()

    def test_prepare_request_eea(self):
        url, headers = self.eea_handler.prepare_request("mock_uid_mock_id")
        self.assertEqual(f"{CLMS_API_URL}/{DOWNLOAD_ENDPOINT}", url)
        self.assertEqual(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": "Bearer mock_token",
            },
            headers,
        )

    def test_find_geo_in_dir(self):
        mock_zip_fs = Mock()
        mock_zip_fs.ls.side_effect = [
            [
                {
                    "name": "folder1",
                },
                {
                    "name": "file1.shp",
                },
            ],
            [
                {
                    "name": "file2.tif",
                }
            ],
        ]
        mock_zip_fs.isdir.side_effect = [True, False, False]
        geo_files = find_geo_in_dir("/", mock_zip_fs)  # Still a static method or
        # helper
        self.assertEqual(geo_files, ["file2.tif"])

    def test_find_geo_in_dir_no_valid_file(self):
        mock_zip_fs = Mock()
        mock_zip_fs.ls.side_effect = [
            [
                {
                    "name": "file1.txt",
                }
            ]
        ]
        mock_zip_fs.isdir.return_value = False
        geo_files = find_geo_in_dir("/", mock_zip_fs)
        self.assertEqual(geo_files, [])

    def test_find_geo_in_dir_recursive(self):
        mock_zip_fs = Mock()
        mock_zip_fs.ls.side_effect = lambda path: {
            "/": [
                {"name": "/folder1"},
                {"name": "/file1.shp"},
            ],
            "/folder1": [
                {"name": "/folder1/subfolder"},
                {"name": "/folder1/file2.tif"},
            ],
            "/folder1/subfolder": [
                {"name": "/folder1/subfolder/file3.txt"},
            ],
        }[path]

        mock_zip_fs.isdir.side_effect = lambda path: path in [
            "/folder1",
            "/folder1/subfolder",
        ]

        result = find_geo_in_dir("/", mock_zip_fs)  # Still a static method or helper

        expected_files = [
            "/folder1/file2.tif",
        ]
        self.assertEqual(expected_files, result)

        mock_zip_fs.ls.assert_any_call("/")
        mock_zip_fs.ls.assert_any_call("/folder1")
        mock_zip_fs.ls.assert_any_call("/folder1/subfolder")

    def test_get_dataset_download_info(self):
        dataset_id = "dataset123"
        file_id = "file456"
        expected_result = {
            "Datasets": [
                {
                    "DatasetID": dataset_id,
                    "FileID": file_id,
                }
            ]
        }
        result = get_dataset_download_info(dataset_id, file_id)
        self.assertEqual(result, expected_result)

    def test_get_authorization_header(self):
        token = "test_token"
        expected = {"Authorization": "Bearer test_token"}
        self.assertEqual(get_authorization_header(token), expected)

    def test_has_expired_not_expired(self):
        download_available_time = (datetime.now() - timedelta(hours=1)).isoformat()
        self.assertFalse(has_expired(download_available_time))

    def test_has_expired_expired(self):
        download_available_time = (
            datetime.now() - timedelta(hours=TIME_TO_EXPIRE + 1)
        ).isoformat()
        self.assertTrue(has_expired(download_available_time))

    def test_preprocess_data_no_files(self):
        dataset_dir = os.path.join(self.eea_handler.download_folder, self.mock_data_id)
        os.makedirs(dataset_dir, exist_ok=True)

        self.eea_handler.preprocess_data(self.mock_data_id)

        data_ids = self.eea_handler.cache_store.list_data_ids()
        self.assertNotIn(self.mock_data_id + ".zarr", data_ids)

    def create_dummy_dataset(self):
        data = xr.Dataset(
            {"band_1": (("y", "x"), [[1, 2], [3, 4]])},
            coords={"y": [0, 1], "x": [0, 1]},
        )
        return data

    @patch.object(EeaProductHandler, "_merge_and_save")
    @patch.object(
        EeaProductHandler, "_prepare_merge", return_value={"dummy": "dummy_path"}
    )
    def test_preprocess_data_multiple_files(
        self, mock_prepare_merge, mock_merge_and_save
    ):
        dataset_dir = os.path.join(self.test_path + "/downloads", self.mock_data_id)

        self.mock_cache_store.fs.sep.join.return_value = dataset_dir
        self.mock_cache_store.fs.ls.side_effect = ("file_1.tif", "file_2.tif")

        self.eea_handler.preprocess_data(self.mock_data_id)

        mock_prepare_merge.assert_called_once()
        mock_merge_and_save.assert_called_once_with(
            mock_prepare_merge.return_value, self.mock_data_id
        )

    @patch("xcube_clms.product_handlers.eea.chunk_dataset")
    def test_preprocess_data_single_file(self, mock_chunk_dataset):
        self.mock_cache_store.fs.ls.return_value = ("file_1.tif",)
        self.mock_cache_store.write_data = MagicMock()
        self.eea_handler.tile_size = (20, 20)
        self.eea_handler.preprocess_data(self.mock_data_id)

        self.eea_handler.cache_store.open_data.assert_called()
        mock_chunk_dataset.assert_called()
        self.eea_handler.cache_store.write_data.assert_called()

    @patch("xcube_clms.product_handlers.eea.rioxarray.open_rasterio")
    def test_merge_and_save_no_files(
        self,
        mock_rioxarray_open_rasterio,
    ):
        self.data_id = "product_empty"
        en_map = defaultdict(list)
        self.eea_handler.tile_size = (20, 20)
        self.eea_handler.cleanup = True
        self.eea_handler._merge_and_save(en_map, self.data_id)

        mock_rioxarray_open_rasterio.assert_not_called()

        self.eea_handler.cleanup = False
        self.eea_handler._merge_and_save(en_map, self.data_id)

        mock_rioxarray_open_rasterio.assert_not_called()
        self.mock_cache_store.write_data.assert_not_called()

    @patch("xcube_clms.product_handlers.eea.rasterio.open")
    @patch("xcube_clms.product_handlers.eea.rioxarray.open_rasterio")
    def test_merge_and_save_single_file(
        self, mock_rioxarray_open_rasterio, mock_rasterio_open
    ):
        ds = xr.Dataset()
        single_array = xr.DataArray(
            [[1, 2, 3], [4, 5, 6]],
            dims=["y", "x"],
            coords={"y": np.arange(2), "x": np.arange(3)},
        )
        ds["band_1"] = single_array
        mock_rioxarray_open_rasterio.return_value = ds

        data_id = "product|dataset"
        en_map = defaultdict(list)
        en_map["E34N78"].append(f"{data_id}/file_1_E34N78.tif")

        mock_rasterio_open.return_value.__enter__.return_value.height = 10000
        mock_rasterio_open.return_value.__enter__.return_value.width = 10000

        self.eea_handler.tile_size = (20, 20)
        self.eea_handler.cleanup = True
        self.eea_handler._merge_and_save(en_map, data_id)

        mock_rioxarray_open_rasterio.assert_called_once()
        self.mock_cache_store.write_data.assert_called_once()

        final_dataset, _ = self.mock_cache_store.write_data.call_args[0]
        self.assertEqual(
            ds.rename(band_1=f"{data_id.split(DATA_ID_SEPARATOR)[-1]}"),
            final_dataset,
        )

    def test_prepare_merge_valid_files(self):
        files = ["file_E12N34.tif", "file_E56N78.tif"]
        data_id = "test_dataset"

        self.eea_handler.tile_size = (20, 20)
        self.eea_handler.fs = fsspec.filesystem("file")
        self.eea_handler.download_folder = f"{self.test_path}/downloads"

        expected_en_map = defaultdict(list)
        expected_en_map["E12N34"].append(
            f"{self.test_path}/downloads/{data_id}/file_E12N34.tif"
        )
        expected_en_map["E56N78"].append(
            f"{self.test_path}/downloads/{data_id}/file_E56N78.tif"
        )

        en_map = self.eea_handler._prepare_merge(files, data_id)
        self.assertEqual(expected_en_map, en_map)

    def test_prepare_merge_invalid_files(self):
        files = ["invalid_file_1.tif", "invalid_file_2.tif"]
        data_id = "test_dataset"

        self.eea_handler.fs = fsspec.filesystem("file")
        self.eea_handler.download_folder = f"{self.test_path}/downloads"

        en_map = self.eea_handler._prepare_merge(files, data_id)
        self.assertEqual(defaultdict(list), en_map)

    def test_prepare_merge_mixed_files(self):
        files = ["valid_E12N34.tif", "invalid_file.tif"]
        data_id = "test_dataset"

        self.eea_handler.fs = fsspec.filesystem("file")
        self.eea_handler.download_folder = f"{self.test_path}/downloads"

        expected_en_map = defaultdict(list)
        expected_en_map["E12N34"].append(
            f"{self.test_path}/downloads/{data_id}/valid_E12N34.tif"
        )

        en_map = self.eea_handler._prepare_merge(files, data_id)
        self.assertEqual(expected_en_map, en_map)


def save_dataset_as_tif(ds: xr.Dataset, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    data = ds["band_1"].values.astype("float32")
    height, width = data.shape

    transform = from_origin(0, 0, 1, 1)

    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=height,
        width=width,
        count=1,
        dtype=data.dtype,
        crs="+proj=latlong",
        transform=transform,
    ) as dst:
        dst.write(data, 1)
