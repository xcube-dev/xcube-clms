import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, Mock

import fsspec
import requests

from xcube_clms.constants import TIME_TO_EXPIRE
from xcube_clms.download_manager import (
    get_dataset_download_info,
    get_authorization_header,
    has_expired,
    DownloadTaskManager,
)


class TestDownloadTaskManager(unittest.TestCase):

    def setUp(self):
        self.mock_token_handler = MagicMock()
        self.mock_token_handler.api_token = "mock_token"
        self.download_manager = DownloadTaskManager(
            token_handler=self.mock_token_handler,
            url="http://mock-api-url",
            path="/mock/path",
        )
        self.download_manager._token_handler = MagicMock()

        self.item = {"@id": "mock_id", "path": "mock_path", "source": "mock_source"}
        self.product = {
            "dataset_download_information": {
                "items": [{"full_source": "valid_source"}]
            },
            "UID": "mock_uid",
        }

        self.mock_fsspec_patcher = patch(
            "xcube_clms.download_manager.fsspec.filesystem"
        )
        self.mock_make_api_request_patcher = patch(
            "xcube_clms.download_manager.make_api_request"
        )
        self.mock_get_response_of_type_patcher = patch(
            "xcube_clms.download_manager.get_response_of_type"
        )
        self.mock_has_expired_patcher = patch("xcube_clms.download_manager.has_expired")

        self.mock_fsspec = self.mock_fsspec_patcher.start()
        self.mock_make_api_request = self.mock_make_api_request_patcher.start()
        self.mock_get_response_of_type = self.mock_get_response_of_type_patcher.start()
        self.mock_has_expired = self.mock_has_expired_patcher.start()

    def tearDown(self):
        patch.stopall()

    def test_existing_complete_request(self):
        self.download_manager.get_current_requests_status = MagicMock()
        self.download_manager.get_current_requests_status.return_value = (
            "COMPLETE",
            "existing_task_id",
        )

        task_id = self.download_manager.request_download(
            "mock_uid", self.item, self.product
        )
        self.assertEqual(task_id, "existing_task_id")
        self.download_manager.get_current_requests_status.assert_called_once_with(
            dataset_id="mock_uid", file_id="mock_id"
        )

    def test_existing_pending_request(self):
        self.download_manager.get_current_requests_status = MagicMock()
        self.download_manager.get_current_requests_status.return_value = (
            "PENDING",
            "existing_task_id",
        )
        task_id = self.download_manager.request_download(
            "mock_uid", self.item, self.product
        )
        self.assertEqual(task_id, "existing_task_id")
        self.download_manager.get_current_requests_status.assert_called_once_with(
            dataset_id="mock_uid", file_id="mock_id"
        )

    def test_new_request(self):
        response = {"TaskIds": [{"TaskID": "mock_task_id"}]}
        self.mock_get_response_of_type.return_value = response
        self.download_manager.get_current_requests_status = MagicMock()
        self.download_manager.get_current_requests_status.return_value = (
            "UNDEFINED",
            "mock_uid",
        )
        task_id = self.download_manager.request_download(
            "mock_uid", self.item, self.product
        )
        self.assertEqual("mock_task_id", task_id)
        self.download_manager.get_current_requests_status.assert_called_once_with(
            dataset_id="mock_uid", file_id="mock_id"
        )

    def test_cancelled_request(self):
        self.download_manager.get_current_requests_status = MagicMock()
        self.download_manager.get_current_requests_status.return_value = (
            "CANCELLED",
            "cancelled_task_id",
        )
        response = {"TaskIds": [{"TaskID": "mock_task_id"}]}
        self.mock_get_response_of_type.return_value = response
        task_id = self.download_manager.request_download(
            "mock_uid", self.item, self.product
        )
        self.assertEqual(task_id, "mock_task_id")

    def test_get_download_url_success(self):
        response = {
            "mock_task_id": {
                "Status": "Finished_ok",
                "DownloadURL": "http://mock-download-url",
                "FileSize": 12345,
            }
        }
        response_mock = Mock(spec=requests.Response)
        response_mock.json.return_value = response
        self.mock_make_api_request.return_value = response_mock
        self.mock_get_response_of_type.return_value = response
        task_id = "mock_task_id"

        result = self.download_manager.get_download_url(task_id)

        self.assertEqual(("http://mock-download-url", 12345), result)
        self.mock_make_api_request.assert_called_once()

    def test_get_download_url_pending_task(self):
        response_data = {"task_id1": {"Status": "In_progress"}}
        self.mock_get_response_of_type.return_value = response_data
        with self.assertRaises(Exception) as cm:
            self.download_manager.get_download_url("task_id1")

    def test_get_download_url_invalid_response(self):
        response_data = {"invalid_key": "invalid_value"}
        self.mock_get_response_of_type.return_value = response_data
        url = self.download_manager.get_download_url("task_id1")
        self.assertEqual(None, url)

    def test_get_current_requests_status_by_task_id(self):
        self.mock_has_expired.return_value = False
        response_data = {
            "task_id1": {
                "Status": "Finished_ok",
                "Datasets": [{"DatasetID": "dataset_id", "FileID": "file_id"}],
                "FinalizationDateTime": "2025-01-01T12:00:00",
            }
        }
        self.mock_get_response_of_type.return_value = response_data

        status, task_id = self.download_manager.get_current_requests_status(
            task_id="task_id1"
        )
        self.assertEqual("COMPLETE", status)
        self.assertEqual("task_id1", task_id)

    def test_get_current_requests_status_with_multiple_same_data_ids(self):
        self.mock_has_expired.return_value = False
        response_data = {
            "task_id1": {
                "Status": "Finished_ok",
                "Datasets": [{"DatasetID": "dataset_id", "FileID": "file_id"}],
                "FinalizationDateTime": "2024-11-01T12:00:00",
            },
            "task_id2": {
                "Status": "In_progress",
                "Datasets": [{"DatasetID": "dataset_id", "FileID": "file_id"}],
                "FinalizationDateTime": "2025-01-01T12:00:00",
            },
            "task_id3": {
                "Status": "Cancelled",
                "Datasets": [{"DatasetID": "dataset_id", "FileID": "file_id"}],
                "FinalizationDateTime": "2025-01-01T12:00:00",
            },
            "task_id4": {
                "Status": "Finished_ok",
                "Datasets": [{"DatasetID": "dataset_id", "FileID": "file_id"}],
                "FinalizationDateTime": "2025-01-01T12:00:00",
            },
        }
        self.mock_get_response_of_type.return_value = response_data

        status, task_id = self.download_manager.get_current_requests_status(
            dataset_id="dataset_id", file_id="file_id"
        )
        self.assertEqual("COMPLETE", status)
        self.assertEqual("task_id4", task_id)

    def test_get_current_requests_status_with_multiple_same_data_ids_nt_complete(self):
        self.mock_has_expired.return_value = False
        response_data = {
            "task_id2": {
                "Status": "In_progress",
                "Datasets": [{"DatasetID": "dataset_id", "FileID": "file_id"}],
                "FinalizationDateTime": "2025-01-01T12:00:00",
            },
            "task_id3": {
                "Status": "Cancelled",
                "Datasets": [{"DatasetID": "dataset_id", "FileID": "file_id"}],
                "FinalizationDateTime": "2025-01-01T12:00:00",
            },
            "task_id4": {
                "Status": "Cancelled",
                "Datasets": [{"DatasetID": "dataset_id", "FileID": "file_id"}],
                "FinalizationDateTime": "2025-01-01T12:00:00",
            },
        }
        self.mock_get_response_of_type.return_value = response_data

        status, task_id = self.download_manager.get_current_requests_status(
            dataset_id="dataset_id", file_id="file_id"
        )
        self.assertEqual("PENDING", status)
        self.assertEqual("task_id2", task_id)

    def test_prepare_download_request(self):
        expected_json = {"Datasets": [{"DatasetID": "mock_uid", "FileID": "mock_id"}]}

        url, headers, json_payload = self.download_manager._prepare_download_request(
            "data_id", self.item, self.product
        )
        print(url, headers)
        self.assertEqual("http://mock-api-url/@datarequest_post", url)
        self.assertEqual(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": "Bearer mock_token",
            },
            headers,
        )
        self.assertEqual(expected_json, json_payload)

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
        geo_files = DownloadTaskManager._find_geo_in_dir("/", mock_zip_fs)
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
        geo_files = DownloadTaskManager._find_geo_in_dir("/", mock_zip_fs)
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

        result = DownloadTaskManager._find_geo_in_dir("/", mock_zip_fs)

        expected_files = [
            "/folder1/file2.tif",
        ]
        self.assertEqual(expected_files, result)

        mock_zip_fs.ls.assert_any_call("/")
        mock_zip_fs.ls.assert_any_call("/folder1")
        mock_zip_fs.ls.assert_any_call("/folder1/subfolder")

    @patch("tempfile.NamedTemporaryFile")
    @patch("builtins.open", create=True)
    def test_download_data(self, mock_dest_open, mock_temp_file):
        mock_temp_file.return_value.__enter__.return_value.name = "/tmp/test"

        mock = Mock()
        mock.iter_content.return_value = [b"chunk1", b"chunk2"]
        self.mock_make_api_request.return_value = mock

        mock_file_fs = Mock(spec=fsspec.AbstractFileSystem)
        mock_file_fs.dirname = Mock(return_value=f"{self.download_manager.path}")

        mock_outer_zip_fs = Mock()
        mock_outer_zip_fs.ls.return_value = [
            {
                "filename": "test.zip",
                "name": "test.zip",
            }
        ]

        mock_inner_zip_fs = Mock()
        mock_inner_zip_fs.ls.return_value = [{"name": "test.tif"}]
        mock_inner_zip_fs.isdir.return_value = False

        self.mock_fsspec.side_effect = lambda protocol, fo=None: {
            "zip": mock_outer_zip_fs if fo == "/tmp/test" else mock_inner_zip_fs,
            "file": mock_file_fs,
        }[protocol]

        outer_file = Mock()
        outer_context = Mock()
        outer_context.__enter__ = Mock(return_value=outer_file)
        outer_context.__exit__ = Mock()
        mock_outer_zip_fs.open.return_value = outer_context

        mock_source_file = Mock()
        mock_source_file.read.side_effect = [b"data", b""]
        inner_context = Mock()
        inner_context.__enter__ = Mock(return_value=mock_source_file)
        inner_context.__exit__ = Mock()
        mock_inner_zip_fs.open.return_value = inner_context

        self.download_manager.download_data("http://test.com", "test_id")

        self.mock_make_api_request.assert_called_once_with(
            "http://test.com", timeout=600, stream=True
        )
        self.assertTrue(mock_file_fs.makedirs.called)
        self.assertTrue(mock_file_fs.dirname.called)
        self.assertTrue(mock_outer_zip_fs.ls.called)
        self.assertTrue(mock_outer_zip_fs.open.called)
        self.assertTrue(mock_inner_zip_fs.ls.called)
        self.assertTrue(mock_inner_zip_fs.open.called)
        self.assertTrue(mock_dest_open.called)


def test_get_dataset_download_info():
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
    assert result == expected_result


def test_get_authorization_header():
    token = "test_token"
    expected = {"Authorization": "Bearer test_token"}
    assert get_authorization_header(token) == expected


def test_has_expired_not_expired():
    download_available_time = (datetime.now() - timedelta(hours=1)).isoformat()
    assert not has_expired(download_available_time)


def test_has_expired_expired():
    download_available_time = (
        datetime.now() - timedelta(hours=TIME_TO_EXPIRE + 1)
    ).isoformat()
    assert has_expired(download_available_time)
