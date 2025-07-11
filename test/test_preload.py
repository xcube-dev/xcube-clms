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

import unittest

import pytest
from unittest.mock import Mock, patch

from xcube.core.store import PreloadState

from xcube_clms.preload import ClmsPreloadHandle


class TestClmsPreloadHandle(unittest.TestCase):

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        self.sample_data_id_maps = {
            "dataset1": {"meta": {"info": "something"}},
            "dataset2": {"meta": {"info": "another"}},
        }

        self.mock_cache_store = Mock()
        self.mock_cache_store.root = str(tmp_path)
        self.mock_cache_store.fs.sep = "/"
        self.mock_cache_store.fs = self.mock_cache_store.fs

        self.mock_preload_data = Mock()

        self.handle = ClmsPreloadHandle(
            data_id_maps=self.sample_data_id_maps,
            cache_store=self.mock_cache_store,
            preload_data=self.mock_preload_data,
            blocking=False,
            silent=True,
        )

    def test_initialization(self):
        self.assertEqual(
            set(self.handle.data_id_maps.keys()), set(self.sample_data_id_maps.keys())
        )
        self.assertEqual(
            f"{self.mock_cache_store.root}/downloads",
            self.handle._download_folder,
        )

    @patch("xcube_clms.preload.cleanup_dir")
    def test_close_calls_notify_and_cleanup(self, mock_cleanup_dir):
        self.handle.notify = Mock()

        self.handle.close()

        expected_states = [
            PreloadState(data_id="dataset1", message="Cleaning up in Progress..."),
            PreloadState(data_id="dataset2", message="Cleaning up in Progress..."),
            PreloadState(data_id="dataset1", message="Cleaning up Finished."),
            PreloadState(data_id="dataset2", message="Cleaning up Finished."),
        ]

        actual_calls = self.handle.notify.call_args_list
        self.assertEqual(len(actual_calls), len(expected_states))

        for actual_call, expected_state in zip(actual_calls, expected_states):
            actual_arg = actual_call.args[0]
            self.assertEqual(actual_arg.data_id, expected_state.data_id)
            self.assertEqual(actual_arg.message, expected_state.message)

        mock_cleanup_dir.assert_called_once_with(self.handle._download_folder)
