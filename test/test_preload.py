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

from xcube_clms.preload import ClmsPreloadHandle


class TestClmsPreloadHandle(unittest.TestCase):

    @patch("xcube_clms.preload.ExecutorPreloadHandle.notify")
    @patch("xcube_clms.preload.DownloadTaskManager")
    @patch("xcube_clms.preload.ClmsApiTokenHandler")
    def test_preload_data_cached(
        self,
        mock_api_token_handler,
        mock_download_task_manager,
        mock_notify,
    ):
        mock_api_token_handler.api_token = MagicMock()
        mock_fs_data_store = MagicMock()
        mock_fs_data_store.list_data_ids.return_value = []
        data_id_maps = {
            "cached_data|id": {
                "item": {"id": "item_id"},
                "product": {"id": "product_id"},
            }
        }
        download_manager_instance = MagicMock()
        mock_download_task_manager.return_value = download_manager_instance
        download_manager_instance.request_download.return_value = "mock_task_id"
        download_manager_instance.get_current_requests_status.return_value = (
            "COMPLETE",
            "",
        )
        download_manager_instance.get_download_url.return_value = ("download_url", "")
        download_manager_instance.download_data = MagicMock()

        preload_handle = ClmsPreloadHandle(
            data_id_maps=data_id_maps,
            url="http://mock-url",
            credentials={},
            cache_store=mock_fs_data_store,
        )

        call_args = mock_notify.call_args

        mock_fs_data_store.list_data_ids.assert_called()
        self.assertIsNotNone(call_args)
        args, _ = call_args
        self.assertEqual(args[0].data_id, "cached_data|id")
        self.assertEqual(args[0].progress, 1.0)
        self.assertEqual(args[0].message, "Preloading Complete.")
