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
            self.assertEqual(mock_file_store, cache_manager.get_file_store())
            self.assertEqual(expected_cache, cache_manager.get_cache())

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
            self.assertEqual(cache_manager.get_cache(), expected_cache)

            mock_listdir.return_value = [
                "product|file1_data",
                "product|file3_data",
            ]
            cache_manager.refresh_cache()
            expected_cache = {
                "product|file1_data": os.path.join(temp_dir, "product|file1_data"),
                "product|file3_data": os.path.join(temp_dir, "product|file3_data"),
            }
            self.assertEqual(cache_manager.get_cache(), expected_cache)

    @patch("xcube_clms.cache_manager.os.listdir")
    @patch("xcube_clms.cache_manager.new_data_store")
    def test_get_cache_when_no_data_id_separator(
        self, mock_new_data_store, mock_listdir
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            mock_file_store = MagicMock()
            mock_new_data_store.return_value = mock_file_store
            mock_listdir.return_value = ["file1", "file2"]
            cache_manager = CacheManager(temp_dir)
            self.assertEqual(cache_manager.get_cache(), {})
