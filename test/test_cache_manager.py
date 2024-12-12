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
import tempfile
import unittest
from unittest.mock import patch, MagicMock

from xcube_clms.cache_manager import CacheManager


class TestCacheManager(unittest.TestCase):

    @patch("xcube_clms.cache_manager.os.makedirs")
    @patch("xcube_clms.cache_manager.os.listdir")
    @patch("xcube_clms.cache_manager.new_data_store")
    def test_init_creates_filestore_and_refreshes_cache(
        self, mock_new_data_store, mock_listdir, mock_makedirs
    ):

        with tempfile.TemporaryDirectory() as temp_dir:
            mock_listdir.return_value = ["product|file1_data", "product|file2_data"]

            mock_file_store = MagicMock()
            mock_new_data_store.return_value = mock_file_store

            cache_manager = CacheManager(temp_dir)

            mock_makedirs.assert_called_once_with(temp_dir, exist_ok=True)
            mock_new_data_store.assert_called_once_with("file", root=temp_dir)
            mock_listdir.assert_called_once_with(temp_dir)

            expected_cache = {
                "product|file1_data": os.path.join(temp_dir, "product|file1_data"),
                "product|file2_data": os.path.join(temp_dir, "product|file2_data"),
            }
            self.assertEqual(mock_file_store, cache_manager.file_store)
            self.assertEqual(expected_cache, cache_manager.cache)

    @patch("xcube_clms.cache_manager.os.listdir")
    @patch("xcube_clms.cache_manager.new_data_store")
    def test_refresh_cache_populates_cache_correctly(
        self, mock_new_data_store, mock_listdir
    ):
        with tempfile.TemporaryDirectory() as temp_dir:

            mock_listdir.return_value = ["product|file1_data", "product|file2_data"]

            mock_file_store = MagicMock()
            mock_new_data_store.return_value = mock_file_store
            cache_manager = CacheManager(temp_dir)
            expected_cache = {
                "product|file1_data": os.path.join(temp_dir, "product|file1_data"),
                "product|file2_data": os.path.join(temp_dir, "product|file2_data"),
            }
            self.assertEqual(cache_manager.cache, expected_cache)

            mock_listdir.return_value = [
                "product|file1_data",
                "product|file3_data",
            ]
            cache_manager.refresh_cache()
            expected_cache = {
                "product|file1_data": os.path.join(temp_dir, "product|file1_data"),
                "product|file3_data": os.path.join(temp_dir, "product|file3_data"),
            }
            self.assertEqual(cache_manager.cache, expected_cache)

    @patch("xcube_clms.cache_manager.os.listdir")
    @patch("xcube_clms.cache_manager.new_data_store")
    def test_cache_when_no_data_id_separator(self, mock_new_data_store, mock_listdir):
        with tempfile.TemporaryDirectory() as temp_dir:
            mock_file_store = MagicMock()
            mock_new_data_store.return_value = mock_file_store
            mock_listdir.return_value = ["file1", "file2"]
            cache_manager = CacheManager(temp_dir)
            self.assertEqual(cache_manager.cache, {})
