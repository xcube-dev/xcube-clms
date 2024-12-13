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


class CacheManagerTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.mock_listdir_patcher = patch("xcube_clms.cache_manager.os.listdir")
        self.mock_new_data_store_patcher = patch(
            "xcube_clms.cache_manager.new_data_store"
        )
        self.mock_makedirs_patcher = patch("xcube_clms.cache_manager.os.makedirs")

        self.mock_listdir = self.mock_listdir_patcher.start()
        self.mock_new_data_store = self.mock_new_data_store_patcher.start()
        self.mock_makedirs = self.mock_makedirs_patcher.start()

        self.mock_file_store = MagicMock()
        self.mock_new_data_store.return_value = self.mock_file_store

    def tearDown(self):
        patch.stopall()
        self.temp_dir.cleanup()

    def create_cache_manager(self):
        return CacheManager(self.temp_dir.name)

    def test_refresh_cache_populates_cache_correctly(self):
        self.mock_listdir.return_value = ["product|file1_data", "product|file2_data"]

        cache_manager = self.create_cache_manager()
        expected_cache = {
            "product|file1_data": os.path.join(
                self.temp_dir.name, "product|file1_data"
            ),
            "product|file2_data": os.path.join(
                self.temp_dir.name, "product|file2_data"
            ),
        }
        self.assertEqual(cache_manager.cache, expected_cache)
        self.assertEqual(self.mock_file_store, cache_manager.file_store)

        self.mock_listdir.return_value = ["product|file1_data", "product|file3_data"]
        cache_manager.refresh_cache()

        expected_cache = {
            "product|file1_data": os.path.join(
                self.temp_dir.name, "product|file1_data"
            ),
            "product|file3_data": os.path.join(
                self.temp_dir.name, "product|file3_data"
            ),
        }
        self.assertEqual(cache_manager.cache, expected_cache)

    def test_cache_when_no_data_id_separator(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            self.mock_listdir.return_value = ["file1", "file2"]
            cache_manager = self.create_cache_manager()
            self.assertEqual(cache_manager.cache, {})
