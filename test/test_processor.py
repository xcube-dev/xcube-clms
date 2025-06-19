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
import os
import shutil
import tempfile
import unittest
from collections import defaultdict
from pathlib import Path
from unittest.mock import patch, MagicMock

import numpy as np
import rasterio
import xarray as xr
from rasterio.transform import from_origin
from xcube.core.store import new_data_store

from xcube_clms.constants import DATA_ID_SEPARATOR
from xcube_clms.processor import FileProcessor
from xcube_clms.processor import cleanup_dir
from xcube_clms.processor import find_easting_northing


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


class FileProcessorTest(unittest.TestCase):
    def setUp(self):
        self.mock_data_id = "product_id|dataset_id"
        self.test_path = "/tmp/test/path"
        if os.path.exists(self.test_path):
            shutil.rmtree(self.test_path)
        os.makedirs(self.test_path)
        self.file_store = new_data_store("file", root=self.test_path)
        self.mock_file_store = MagicMock()

    def tearDown(self):
        shutil.rmtree(self.test_path)

    def create_dummy_dataset(self):
        data = xr.Dataset(
            {"band_1": (("y", "x"), [[1, 2], [3, 4]])},
            coords={"y": [0, 1], "x": [0, 1]},
        )
        return data

    def test_preprocess_eea_datasets_no_files(self):
        processor = FileProcessor(self.file_store, cleanup=True)
        processor.tile_size = (1, 1)
        processor.download_folder = self.test_path + "/downloads"
        processor.fs = self.file_store.fs
        processor.cache_store = self.file_store

        dataset_dir = os.path.join(self.test_path, "downloads", self.mock_data_id)
        os.makedirs(dataset_dir, exist_ok=True)

        processor.preprocess_eea_datasets(self.mock_data_id)

        data_ids = self.file_store.list_data_ids()
        self.assertNotIn(self.mock_data_id + ".zarr", data_ids)

    @patch.object(FileProcessor, "_merge_and_save")
    @patch.object(FileProcessor, "_prepare_merge", return_value={"dummy": "map"})
    def test_preprocess_eea_datasets_multiple_files(
        self, mock_prepare_merge, mock_merge_and_save
    ):
        processor = FileProcessor(self.file_store, cleanup=True)
        processor.tile_size = (1, 1)
        processor.download_folder = self.test_path + "/downloads"
        processor.fs = self.file_store.fs
        processor.cache_store = self.file_store

        dataset_dir = os.path.join(self.test_path, "downloads", self.mock_data_id)
        os.makedirs(dataset_dir, exist_ok=True)

        for i in range(2):
            file_path = os.path.join(dataset_dir, f"file_{i}.tif")
            save_dataset_as_tif(self.create_dummy_dataset(), file_path)

        processor.preprocess_eea_datasets(self.mock_data_id)

        mock_prepare_merge.assert_called_once()
        mock_merge_and_save.assert_called_once_with(
            mock_prepare_merge.return_value, self.mock_data_id
        )

    def test_preprocess_eea_datasets_single_file(self):
        processor = FileProcessor(self.file_store, cleanup=True)
        processor.tile_size = (1, 1)
        processor.download_folder = self.test_path + "/downloads"
        processor.fs = self.file_store.fs
        processor.cache_store = self.file_store

        dataset_dir = os.path.join(self.test_path, "downloads", self.mock_data_id)
        os.makedirs(dataset_dir, exist_ok=True)

        file_name = "file_1.tif"
        full_data_id = os.path.join(dataset_dir, file_name)

        save_dataset_as_tif(self.create_dummy_dataset(), full_data_id)

        processor.preprocess_eea_datasets(self.mock_data_id)

        final_data_id = self.mock_data_id + ".zarr"
        data_ids = self.file_store.list_data_ids()

        self.assertIn(final_data_id, data_ids)

        final_dataset = self.file_store.open_data(final_data_id)
        self.assertIsInstance(final_dataset, xr.Dataset)
        self.assertEqual(final_dataset["dataset_id"].shape, (2, 2))
        self.assertIsNotNone(final_dataset.chunks)

        remaining_files = self.file_store.fs.ls(self.test_path)
        only_zarr = [f for f in remaining_files if f.endswith(".zarr")]
        self.assertEqual(
            len(only_zarr),
            1,
            f"Expected only .zarr after cleanup, found: {remaining_files}",
        )

    def test_preprocess_eea_datasets_single_file_cleanup_false(self):
        processor = FileProcessor(self.file_store, cleanup=False)
        processor.tile_size = (1, 1)
        processor.download_folder = self.test_path + "/downloads"
        processor.fs = self.file_store.fs
        processor.cache_store = self.file_store

        dataset_dir = os.path.join(self.test_path, "downloads", self.mock_data_id)
        os.makedirs(dataset_dir, exist_ok=True)

        file_name = "file_1.tif"
        full_data_id = os.path.join(dataset_dir, file_name)

        save_dataset_as_tif(self.create_dummy_dataset(), full_data_id)

        processor.preprocess_eea_datasets(self.mock_data_id)

        final_data_id = self.mock_data_id + ".zarr"
        data_ids = self.file_store.list_data_ids()

        self.assertIn(final_data_id, data_ids)

        final_dataset = self.file_store.open_data(final_data_id)
        self.assertIsInstance(final_dataset, xr.Dataset)
        self.assertEqual(final_dataset["dataset_id"].shape, (2, 2))
        self.assertIsNotNone(final_dataset.chunks)

        remaining_files = self.file_store.fs.ls(self.test_path, recursive=True)
        only_zarr = [f for f in remaining_files if f.endswith(".zarr")]
        self.assertNotEqual(
            len(only_zarr),
            len(remaining_files),
        )

    def test_preprocess_legacy_datasets_functional(self):
        processor = FileProcessor(self.file_store)
        processor.cache_store = self.file_store

        raw_data_id = self.mock_data_id + "_raw.zarr"
        final_data_id = self.mock_data_id + ".zarr"

        raw_dataset = self.create_dummy_dataset()
        self.file_store.write_data(raw_dataset, raw_data_id, replace=True)

        processor.preprocess_legacy_datasets(self.mock_data_id)

        data_ids = self.file_store.list_data_ids()
        self.assertIn(final_data_id, data_ids)
        final_dataset = self.file_store.open_data(final_data_id)
        self.assertIsInstance(final_dataset, xr.Dataset)
        self.assertEqual(final_dataset["band_1"].shape, (2, 2))
        self.assertIsNotNone(final_dataset.chunks)

        self.assertNotIn(raw_data_id, data_ids)

    @patch("xcube_clms.processor.rasterio.open")
    @patch("xcube_clms.processor.rioxarray.open_rasterio")
    @patch("xcube_clms.processor.xr.concat")
    def test_preprocess_merge_and_save(
        self, mock_xr_concat, mock_rioxarray_open_rasterio, mock_rasterio_open
    ):
        dsa = xr.Dataset()
        a = xr.DataArray(
            [[1, 2, 3, 4, 5], [6, 7, 8, 9, 10]],
            dims=["y", "x"],
            coords={"y": np.arange(2), "x": np.arange(5)},
        )
        dsa["band_1"] = a

        dsb = xr.Dataset()
        b = xr.DataArray(
            [[11, 12, 13, 14, 15], [16, 17, 18, 19, 20]],
            dims=["y", "x"],
            coords={"y": np.arange(2, 4), "x": np.arange(5)},
        )
        dsb["band_1"] = b

        dsc = xr.Dataset()
        c = xr.DataArray(
            [[21, 22, 23, 24, 25], [26, 27, 28, 29, 30]],
            dims=["y", "x"],
            coords={"y": np.arange(2), "x": np.arange(5, 10)},
        )
        dsc["band_1"] = c

        y_concat = xr.concat([dsa, dsb], dim="y")
        x_concat = xr.concat([dsc, y_concat], dim="x")

        mock_rioxarray_open_rasterio.side_effect = [dsa, dsb, dsc]
        mock_rasterio_open.return_value.__enter__.return_value.height = 10000
        mock_rasterio_open.return_value.__enter__.return_value.width = 10000

        data_id = "product_1|file_id_1"
        en_map = defaultdict(list)
        en_map["E34N78"].append(f"{data_id}/file_1_E34N78.tif")
        en_map["E35N78"].append(f"{data_id}/file_2_E35N78.tif")
        en_map["E34N79"].append(f"{data_id}/file_3_E34N79.tif")

        processor = FileProcessor(self.mock_file_store, cleanup=True)
        mock_xr_concat.reset_mock()
        processor._merge_and_save(en_map, data_id)

        self.assertEqual(3, mock_rioxarray_open_rasterio.call_count)
        self.assertEqual(3, mock_xr_concat.call_count)

        self.mock_file_store.write_data.assert_called_once()

        args, _ = self.mock_file_store.write_data.call_args
        final_dataset, output_path = args
        self.assertEqual(
            x_concat.rename(band_1=f"{data_id.split(DATA_ID_SEPARATOR)[-1]}").chunk(
                2000
            ),
            final_dataset,
        )

    @patch("xcube_clms.processor.rioxarray.open_rasterio")
    def test_merge_and_save_no_files(
        self,
        mock_rioxarray_open_rasterio,
    ):
        self.data_id = "product_empty"
        en_map = defaultdict(list)

        processor = FileProcessor(
            self.mock_file_store, cleanup=True, tile_size=(2000, 2000)
        )
        processor._merge_and_save(en_map, self.data_id)

        mock_rioxarray_open_rasterio.assert_not_called()

        processor = FileProcessor(self.mock_file_store, cleanup=False)
        processor._merge_and_save(en_map, self.data_id)

        mock_rioxarray_open_rasterio.assert_not_called()
        self.mock_file_store.write_data.assert_not_called()

    @patch("xcube_clms.processor.rasterio.open")
    @patch("xcube_clms.processor.rioxarray.open_rasterio")
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

        processor = FileProcessor(self.mock_file_store, cleanup=True)
        processor._merge_and_save(en_map, data_id)

        mock_rioxarray_open_rasterio.assert_called_once()
        self.mock_file_store.write_data.assert_called_once()

        final_dataset, _ = self.mock_file_store.write_data.call_args[0]
        self.assertEqual(
            ds.rename(band_1=f"{data_id.split(DATA_ID_SEPARATOR)[-1]}"),
            final_dataset,
        )

    def test_prepare_merge_valid_files(self):
        files = ["file_E12N34.tif", "file_E56N78.tif"]
        data_id = "test_dataset"

        processor = FileProcessor(self.file_store)

        expected_en_map = defaultdict(list)
        expected_en_map["E12N34"].append(
            f"{self.test_path}/downloads/{data_id}/file_E12N34.tif"
        )
        expected_en_map["E56N78"].append(
            f"{self.test_path}/downloads/{data_id}/file_E56N78.tif"
        )

        en_map = processor._prepare_merge(files, data_id)
        self.assertEqual(expected_en_map, en_map)

    def test_prepare_merge_invalid_files(self):
        files = ["invalid_file_1.tif", "invalid_file_2.tif"]
        data_id = "test_dataset"
        processor = FileProcessor(self.file_store)

        en_map = processor._prepare_merge(files, data_id)
        self.assertEqual(defaultdict(list), en_map)

    def test_prepare_merge_mixed_files(self):
        files = ["valid_E12N34.tif", "invalid_file.tif"]
        data_id = "test_dataset"
        processor = FileProcessor(self.file_store)

        expected_en_map = defaultdict(list)
        expected_en_map["E12N34"].append(
            f"{self.test_path}/downloads/{data_id}/valid_E12N34.tif"
        )

        en_map = processor._prepare_merge(files, data_id)
        self.assertEqual(expected_en_map, en_map)

    def test_cleanup_dir_deletes_files(self):
        with tempfile.TemporaryDirectory() as tmp_path_str:
            tmp_path = Path(tmp_path_str)
            folder_path = tmp_path / "test_folder"
            folder_path.mkdir()

            delete_file = folder_path / "file1.tif"
            keep_file = folder_path / "file2.zarr"
            delete_file.write_text("test")
            keep_file.write_text("test")

            cleanup_dir(folder_path, keep_extension=".zarr")

            self.assertFalse(delete_file.exists())
            self.assertTrue(keep_file.exists())

    def test_cleanup_dir_deletes_all_files_without_keep_extension(self):
        with tempfile.TemporaryDirectory() as tmp_path_str:
            tmp_path = Path(tmp_path_str)
            folder_path = tmp_path / "test_folder"
            folder_path.mkdir()

            keep_file = folder_path / "file1.zarr"
            delete_file = folder_path / "file2.tif"
            keep_file.write_text("test")
            delete_file.write_text("test")

            cleanup_dir(folder_path)

            self.assertFalse(keep_file.exists())
            self.assertFalse(delete_file.exists())

    def test_cleanup_dir_deletes_nested_directories(self):
        with tempfile.TemporaryDirectory() as tmp_path_str:
            tmp_path = Path(tmp_path_str)
            folder_path = tmp_path / "test_folder"
            folder_path.mkdir()
            nested_folder = folder_path / "nested_folder"
            nested_folder.mkdir()

            nested_file = nested_folder / "file.txt"
            nested_file.write_text("test")

            cleanup_dir(folder_path)

            self.assertFalse(nested_folder.exists())
            self.assertFalse(nested_file.exists())

    def test_cleanup_dir_non_dir(self):
        with tempfile.TemporaryDirectory() as tmp_path_str:
            tmp_path = Path(tmp_path_str)
            folder_path = tmp_path / "test_folder"
            folder_path.mkdir()

            delete_file = folder_path / "file1.tif"
            delete_file.write_text("test")

            with self.assertRaises(ValueError):
                cleanup_dir(delete_file, keep_extension=".zarr")

    def test_find_easting_northing_valid(self):
        name = "randomE12N34text"
        self.assertEqual("E12N34", find_easting_northing(name))

        name = "E12N34"
        self.assertEqual("E12N34", find_easting_northing(name))

    def test_find_easting_northing_invalid(self):
        name = "random_text_without_coordinates"
        self.assertEqual(None, find_easting_northing(name))

    @patch("xcube_clms.processor.fsspec.filesystem")
    def test_get_chunk_size(self, mock_fsspec):
        test_cases = [
            (2000, 2000, {"x": 2000, "y": 2000}),
            (200, 200, {"x": 200, "y": 200}),
            (250, 150, {"x": 250, "y": 150}),
            (99, 99, {"x": 99, "y": 99}),
            (10000, 10000, {"x": 2000, "y": 2000}),
            (5000, 5000, {"x": 1667, "y": 1667}),
        ]

        mock_fsspec.return_value.ls.return_value = []
        processor = FileProcessor(self.mock_file_store, cleanup=True)

        for size_x, size_y, expected in test_cases:
            self.assertEqual(expected, processor._get_chunk_size(size_x, size_y))
