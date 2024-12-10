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
from collections import defaultdict
from unittest.mock import patch, MagicMock

import numpy as np
import xarray as xr

from xcube_clms.constants import DATA_ID_SEPARATOR
from xcube_clms.processor import FileProcessor


class TestProcessor(unittest.TestCase):
    @patch("xcube_clms.processor.LOG")
    @patch("xcube_clms.processor.os.listdir")
    @patch("xcube_clms.processor.cleanup_dir")
    def test_postprocess_single_file(self, mock_cleanup_dir, mock_listdir, mock_log):
        mock_path = "/mock/path"
        mock_data_id = "product_id|dataset_id"
        mock_file_store = MagicMock()

        mock_listdir.return_value = ["file_1.tif"]

        processor = FileProcessor(mock_path, mock_file_store, cleanup=True)
        processor.postprocess(mock_data_id)

        mock_log.info.assert_called_once_with("No postprocessing required.")
        mock_cleanup_dir.assert_not_called()

        mock_log.reset_mock()
        processor = FileProcessor(mock_path, mock_file_store, cleanup=False)
        processor.postprocess(mock_data_id)

        mock_log.info.assert_called_once_with("No postprocessing required.")
        mock_cleanup_dir.assert_not_called()

    @patch("xcube_clms.processor.LOG")
    @patch("xcube_clms.processor.os.listdir")
    @patch("xcube_clms.processor.cleanup_dir")
    def test_postprocess_no_files(self, mock_cleanup_dir, mock_listdir, mock_log):
        mock_path = "/mock/path"
        mock_data_id = "product_id|dataset_id"
        mock_file_store = MagicMock()

        mock_listdir.return_value = []

        processor = FileProcessor(mock_path, mock_file_store, cleanup=True)
        processor.postprocess(mock_data_id)

        mock_log.warn.assert_called_once_with("No files to postprocess!")
        mock_cleanup_dir.assert_not_called()

    @patch("xcube_clms.processor.LOG")
    @patch("xcube_clms.processor.os.listdir")
    @patch("xcube_clms.processor.cleanup_dir")
    @patch("xcube_clms.processor.find_easting_northing")
    def test_postprocess_unsupported_naming(
        self, mock_find_easting_northing, mock_cleanup_dir, mock_listdir, mock_log
    ):
        mock_path = "/mock/path"
        mock_file_store = MagicMock()
        mock_files = ["file_1.tif", "file_2.tif", "file_3.tif"]

        mock_listdir.return_value = mock_files
        mock_find_easting_northing.return_value = None

        processor = FileProcessor(mock_path, mock_file_store, cleanup=True)
        processor.postprocess("invalid_data_id")

        mock_log.error.assert_called_once_with(
            "This naming format is not supported. Currently only filenames with "
            "Eastings and Northings are supported."
        )
        mock_cleanup_dir.assert_not_called()
        mock_file_store.write_data.assert_not_called()

    @patch("xcube_clms.processor.rioxarray.open_rasterio")
    @patch("xcube_clms.processor.xr.concat")
    def test_postprocess_merge_and_save(
        self,
        mock_xr_concat,
        mock_rioxarray_open_rasterio,
    ):
        mock_path = "/mock/path"
        mock_file_store = MagicMock()

        a = xr.DataArray(
            [[1, 2, 3, 4, 5], [6, 7, 8, 9, 10]],
            dims=["y", "x"],
            coords={"y": np.arange(2), "x": np.arange(5)},
        )

        b = xr.DataArray(
            [[11, 12, 13, 14, 15], [16, 17, 18, 19, 20]],
            dims=["y", "x"],
            coords={"y": np.arange(2, 4), "x": np.arange(5)},
        )

        c = xr.DataArray(
            [[21, 22, 23, 24, 25], [26, 27, 28, 29, 30]],
            dims=["y", "x"],
            coords={"y": np.arange(2), "x": np.arange(5, 10)},
        )

        y_concat = xr.concat([a, b], dim="y")
        x_concat = xr.concat([c, y_concat], dim="x")

        mock_rioxarray_open_rasterio.side_effect = [a, b, c]

        data_id = "product_1|file_id_1"
        en_map = defaultdict(list)
        en_map["E34N78"].append(f"{data_id}/file_1_E34N78.tif")
        en_map["E35N78"].append(f"{data_id}/file_2_E35N78.tif")
        en_map["E34N79"].append(f"{data_id}/file_3_E34N79.tif")

        processor = FileProcessor(mock_path, mock_file_store, cleanup=True)
        mock_xr_concat.reset_mock()
        processor._merge_and_save(en_map, data_id)

        assert mock_rioxarray_open_rasterio.call_count == 3
        assert mock_xr_concat.call_count == 3

        mock_file_store.write_data.assert_called_once()

        args, _ = mock_file_store.write_data.call_args
        final_dataset, output_path = args
        assert x_concat.equals(
            final_dataset
        ), "Final output does not match expected x_concat"
        print(output_path, final_dataset)

    @patch("xcube_clms.processor.rioxarray.open_rasterio")
    def test_merge_and_save_no_files(self, mock_rioxarray_open_rasterio):
        mock_path = "/mock/path"
        mock_file_store = MagicMock()
        data_id = "product_empty"
        en_map = defaultdict(list)

        processor = FileProcessor(mock_path, mock_file_store, cleanup=True)
        processor._merge_and_save(en_map, data_id)

        mock_rioxarray_open_rasterio.assert_not_called()
        mock_file_store.write_data.assert_not_called()

        processor = FileProcessor(mock_path, mock_file_store, cleanup=False)
        processor._merge_and_save(en_map, data_id)

        mock_rioxarray_open_rasterio.assert_not_called()
        mock_file_store.write_data.assert_not_called()

    @patch("xcube_clms.processor.rioxarray.open_rasterio")
    def test_merge_and_save_single_file(self, mock_rioxarray_open_rasterio):
        mock_path = "/mock/path"
        mock_file_store = MagicMock()

        single_array = xr.DataArray(
            [[1, 2, 3], [4, 5, 6]],
            dims=["y", "x"],
            coords={"y": np.arange(2), "x": np.arange(3)},
        )
        mock_rioxarray_open_rasterio.return_value = single_array

        data_id = "product|dataset"
        en_map = defaultdict(list)
        en_map["E34N78"].append(f"{data_id}/file_1_E34N78.tif")

        processor = FileProcessor(mock_path, mock_file_store, cleanup=True)
        processor._merge_and_save(en_map, data_id)

        mock_rioxarray_open_rasterio.assert_called_once()
        mock_file_store.write_data.assert_called_once()

        final_dataset, _ = mock_file_store.write_data.call_args[0]
        assert single_array.to_dataset(
            name=f"{data_id.split(DATA_ID_SEPARATOR)[-1]}"
        ).equals(final_dataset)

    @patch("xcube_clms.processor.find_easting_northing")
    def test_prepare_merge_valid_files(self, mock_find_easting_northing):
        mock_find_easting_northing.side_effect = lambda name: {
            "file_E12N34.tif": "E12N34",
            "file_E56N78.tif": "E56N78",
        }.get(name, None)

        files = ["file_E12N34.tif", "file_E56N78.tif"]
        data_id = "test_dataset"
        test_path = "/test/path"
        processor = FileProcessor(test_path, None)

        expected_en_map = defaultdict(list)
        expected_en_map["E12N34"].append(
            os.path.join(test_path, data_id, "file_E12N34.tif")
        )
        expected_en_map["E56N78"].append(
            os.path.join(test_path, data_id, "file_E56N78.tif")
        )

        en_map = processor._prepare_merge(files, data_id)
        assert en_map == expected_en_map

    @patch("xcube_clms.processor.find_easting_northing")
    def test_prepare_merge_invalid_files(self, mock_find_easting_northing):
        mock_find_easting_northing.return_value = None

        files = ["invalid_file_1.tif", "invalid_file_2.tif"]
        data_id = "test_dataset"
        test_path = "/test/path"
        processor = FileProcessor(test_path, None)

        en_map = processor._prepare_merge(files, data_id)
        assert en_map == defaultdict(list)

    @patch("xcube_clms.processor.find_easting_northing")
    def test_prepare_merge_mixed_files(self, mock_find_easting_northing):
        mock_find_easting_northing.side_effect = lambda name: {
            "valid_E12N34.tif": "E12N34",
        }.get(name, None)

        files = ["valid_E12N34.tif", "invalid_file.tif"]
        data_id = "test_dataset"
        test_path = "/test/path"
        processor = FileProcessor(test_path, None)

        expected_en_map = defaultdict(list)
        expected_en_map["E12N34"].append(
            os.path.join(test_path, data_id, "valid_E12N34.tif")
        )

        en_map = processor._prepare_merge(files, data_id)

        assert en_map == expected_en_map
