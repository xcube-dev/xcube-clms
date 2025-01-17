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
import time
import unittest
from unittest.mock import patch, MagicMock

from xcube.core.store import PreloadStatus

from xcube_clms.preload import ClmsPreloadHandle


class TestClmsPreloadHandle(unittest.TestCase):

    def setUp(self):
        self.mock_notify_patcher = patch(
            "xcube_clms.preload.ExecutorPreloadHandle.notify"
        )
        self.mock_download_task_manager_patcher = patch(
            "xcube_clms.preload.DownloadTaskManager"
        )
        self.mock_api_token_handler_patcher = patch(
            "xcube_clms.preload.ClmsApiTokenHandler"
        )
        self.mock_file_processor_patcher = patch("xcube_clms.preload.FileProcessor")
        self.mock_cleanup_dir_patcher = patch("xcube_clms.preload.cleanup_dir")

        self.mock_notify = self.mock_notify_patcher.start()
        self.mock_download_task_manager = (
            self.mock_download_task_manager_patcher.start()
        )
        self.mock_api_token_handler = self.mock_api_token_handler_patcher.start()
        self.mock_file_processor = self.mock_file_processor_patcher.start()
        self.mock_cleanup_dir = self.mock_cleanup_dir_patcher.start()

        self.mock_api_token_handler.return_value.api_token = "test_token"

        self.mock_download_manager = MagicMock()
        self.mock_download_manager.request_download.return_value = "task_123"
        self.mock_download_manager.get_current_requests_status.return_value = (
            "COMPLETE",
            "",
        )
        self.mock_download_manager.get_download_url.return_value = ("download_url", "")
        self.mock_download_task_manager.return_value = self.mock_download_manager

        self.mock_file_processor_instance = MagicMock()
        self.mock_file_processor.return_value = self.mock_file_processor_instance

        self.data_id_maps = {
            "test_data_id": {
                "item": {"id": "item_123"},
                "product": {"id": "product_456"},
            }
        }
        self.url = "http://mock-url.com"
        self.mock_fs_data_store = MagicMock()
        self.mock_fs_data_store.root = "/cache/root"
        self.mock_fs_data_store.fs = MagicMock()
        self.mock_fs_data_store.list_data_ids.return_value = []

    def tearDown(self):
        patch.stopall()

    def test_init(self):
        handle = ClmsPreloadHandle(
            data_id_maps=self.data_id_maps,
            url=self.url,
            credentials={"client_id": "test"},
            cache_store=self.mock_fs_data_store,
            cleanup=True,
        )

        self.assertEqual(handle.data_id_maps, self.data_id_maps)
        self.assertEqual(handle._url, self.url)
        self.assertEqual(handle.cache_store, self.mock_fs_data_store)
        self.assertTrue(handle.cleanup)

        self.mock_api_token_handler.assert_called_once_with({"client_id": "test"})
        self.mock_download_task_manager.assert_called_once()
        self.mock_file_processor.assert_called_once()

    def test_preload_data_cached(self):
        self.mock_fs_data_store.list_data_ids.return_value = ["test_data_id"]

        ClmsPreloadHandle(
            data_id_maps=self.data_id_maps,
            url=self.url,
            credentials={},
            cache_store=self.mock_fs_data_store,
        )
        # wait for threads to finish
        time.sleep(2)
        self.mock_notify.assert_called()
        notification = self.mock_notify.call_args[0][0]
        self.assertEqual(notification.data_id, "test_data_id")
        self.assertEqual(notification.status, PreloadStatus.stopped)
        self.assertEqual(notification.progress, 1.0)
        self.assertIn("already cached", notification.message)

    def test_preload_data_new(self):
        ClmsPreloadHandle(
            data_id_maps=self.data_id_maps,
            url=self.url,
            credentials={},
            cache_store=self.mock_fs_data_store,
        )

        self.mock_download_manager.request_download.assert_called_once_with(
            data_id="test_data_id",
            item={"id": "item_123"},
            product={"id": "product_456"},
        )
        self.mock_download_manager.get_current_requests_status.assert_called()
        self.mock_download_manager.get_download_url.assert_called_once_with("task_123")
        self.mock_download_manager.download_data.assert_called_once()
        # wait for threads to finish
        time.sleep(3)
        notification_calls = self.mock_notify.call_args_list
        self.assertGreaterEqual(len(notification_calls), 4)

        final_notification = notification_calls[-1][0][0]
        self.assertEqual(final_notification.data_id, "test_data_id")
        self.assertEqual(final_notification.progress, 1.0)
        self.assertEqual(
            final_notification.message, "Task ID task_123: Preloading Complete."
        )
        self.mock_file_processor_instance.preprocess.assert_called_once()

    def test_preload_data_cancel(self):
        self.mock_notify.reset_mock()
        self.mock_download_manager.get_current_requests_status.return_value = (
            "CANCELLED",
            "",
        )
        ClmsPreloadHandle(
            data_id_maps=self.data_id_maps,
            url=self.url,
            credentials={},
            cache_store=self.mock_fs_data_store,
        )

        self.mock_download_manager.request_download.assert_called_once_with(
            data_id="test_data_id",
            item={"id": "item_123"},
            product={"id": "product_456"},
        )
        self.mock_download_manager.get_current_requests_status.assert_called()
        # wait for threads to finish
        time.sleep(2)
        notification_calls = self.mock_notify.call_args_list
        self.assertEqual(notification_calls[-1][0][0].data_id, "test_data_id")
        self.assertEqual(notification_calls[-1][0][0].status, PreloadStatus.stopped)
        self.assertIn(
            "Task ID task_123: Download request was cancelled",
            notification_calls[-2][0][0].message,
        )

    def test_close(self):
        handle = ClmsPreloadHandle(
            data_id_maps=self.data_id_maps,
            url=self.url,
            credentials={},
            cache_store=self.mock_fs_data_store,
        )
        self.mock_notify.reset_mock()
        handle.close()

        self.mock_cleanup_dir.assert_called_once_with(self.mock_fs_data_store.root)

        self.assertEqual(self.mock_notify.call_count, 2)
        notifications = [call[0][0] for call in self.mock_notify.call_args_list]
        self.assertIn("Cleaning up in Progress", notifications[0].message)
        self.assertIn("Cleaning up Finished", notifications[1].message)
