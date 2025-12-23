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
from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import xarray as xr
from xcube.core.store import DataStoreError

from xcube_clms.product_handlers.cdse import (CdseProductHandler,
                                              _append_nc_file,
                                              _generate_daily_ssm_paths,
                                              _get_min_max_date)


class TestCdseProductHandler(unittest.TestCase):
    def setUp(self):
        self.mock_api_token_handler = MagicMock()
        self.mock_api_token_handler.api_token = "mock_token"

        self.mock_datasets_info = [
            {
                "id": "mock_uid_mock_id",
                "UID": "mock_uid",
                "dataset_download_information": {
                    "items": [
                        {
                            "@id": "mock_id",
                            "path": "mock_path",
                            "full_source": "EEA",
                            "name": "raster",
                        }
                    ]
                },
            },
            {
                "id": "daily-surface-soil-moisture-v1.0",
                "UID": "mock_uid",
                "dataset_download_information": {
                    "items": [
                        {
                            "@id": "mock_id",
                            "path": "mock_path",
                            "full_source": "CDSE",
                            "name": "raster",
                        }
                    ]
                },
                "characteristics_temporal_extent": "2024-2024",
            },
            {
                "id": "mock_uid_cdse_present_mock_id",
                "UID": "mock_uid",
                "dataset_download_information": {
                    "items": [
                        {
                            "@id": "mock_id",
                            "path": "mock_path",
                            "full_source": "CDSE",
                            "name": "raster",
                        }
                    ]
                },
                "characteristics_temporal_extent": "2024-present",
            },
            {
                "id": "dataset_id",
                "UID": "dataset_id",
                "dataset_download_information": {
                    "items": [
                        {
                            "@id": "file_id",
                            "path": "mock_path",
                            "full_source": "EEA",
                            "name": "raster",
                        }
                    ]
                },
                "downloadable_files": {
                    "items": [{"file": "file_id", "@id": "file_@id"}]
                },
            },
            {
                "id": "unsupported_vector_product",
                "UID": "mock_uid",
                "dataset_download_information": {
                    "items": [
                        {
                            "@id": "mock_id",
                            "path": "mock_path",
                            "full_source": "CDSE",
                            "name": "vector",
                        }
                    ]
                },
                "characteristics_temporal_extent": "2024-present",
            },
        ]

        self.cdse_handler = CdseProductHandler(
            cache_store=None,
            datasets_info=self.mock_datasets_info,
            api_token_handler=self.mock_api_token_handler,
        )

    def test_product_type(self):
        self.assertEqual("cdse", self.cdse_handler.product_type())

    def test_get_open_data_params_schema(self):
        schema = self.cdse_handler.get_open_data_params_schema()
        self.assertIn("time_range", schema.properties)

    def test_get_data_id_non_raster(self):
        item = self.mock_datasets_info[2]
        result = list(self.cdse_handler.get_data_id(item=item))
        self.assertEqual(result, [])

    def test_get_data_id_unsupported_product(self):
        item = self.mock_datasets_info[2]
        result = list(self.cdse_handler.get_data_id(item=item))
        self.assertEqual(result, [])

    def test_get_data_id_supported_no_attrs(self):
        item = self.mock_datasets_info[1]
        result = list(self.cdse_handler.get_data_id(item=item))
        self.assertEqual(result, ["daily-surface-soil-moisture-v1.0"])

    def test_get_data_id_supported_with_all_attrs(self):
        item = self.mock_datasets_info[1]
        result = list(self.cdse_handler.get_data_id(item=item, include_attrs=True))
        data_id, attrs = result[0]
        self.assertEqual(data_id, "daily-surface-soil-moisture-v1.0")
        self.assertIn("path", attrs)

    def test_get_data_id_supported_with_filtered_attrs(self):
        item = self.mock_datasets_info[1]
        result = list(self.cdse_handler.get_data_id(item=item, include_attrs=["path"]))
        _, attrs = result[0]
        self.assertEqual(attrs, {"path": "mock_path"})

    @patch("xcube_clms.product_handlers.cdse._get_df_from_data_id")
    def test_describe_data(self, mock_get_df):
        df = pd.DataFrame(
            {
                "content_date_start": ["2024-01-01", "2024-01-02"],
                "s3_path": ["a", "b"],
                "bbox": "POLYGON((-11 72,-11 35,50 35,50 72,-11 72))",
            }
        )
        mock_get_df.return_value = df

        descriptor = self.cdse_handler.describe_data(
            "daily-surface-soil-moisture-v1.0",
            self.mock_datasets_info[1],
        )

        self.assertEqual(descriptor.data_id, "daily-surface-soil-moisture-v1.0")
        self.assertEqual(descriptor.time_range, ("2024-01-01", "2024-01-02"))
        self.assertEqual(descriptor.bbox, (-11.0, 35.0, 50.0, 72.0))

    def test_open_data_no_time_range(self):
        with self.assertRaises(DataStoreError):
            self.cdse_handler.open_data("daily-surface-soil-moisture-v1.0")

    @patch(
        "xcube_clms.product_handlers.cdse._generate_daily_ssm_paths",
        return_value=[],
    )
    def test_open_data_no_urls_found(self, _):
        with self.assertRaises(DataStoreError):
            self.cdse_handler.open_data(
                "daily-surface-soil-moisture-v1.0",
                time_range=("2024-01-01", "2024-01-02"),
            )

    @patch(
        "xcube_clms.product_handlers.cdse.detect_format",
        return_value="tiff",
    )
    @patch(
        "xcube_clms.product_handlers.cdse._generate_daily_ssm_paths",
        return_value=["/path/file.tif"],
    )
    def test_open_data_unsupported_format(self, *_):
        with self.assertRaises(DataStoreError):
            self.cdse_handler.open_data(
                "daily-surface-soil-moisture-v1.0",
                time_range=("2024-01-01", "2024-01-02"),
            )

    @patch(
        "xcube_clms.product_handlers.cdse.detect_format",
        return_value="netcdf",
    )
    @patch(
        "xcube_clms.product_handlers.cdse._generate_daily_ssm_paths",
        return_value=["/path/a.nc", "/path/b.nc"],
    )
    @patch("xcube_clms.product_handlers.cdse.rioxarray.open_rasterio")
    def test_open_data_netcdf_success(
        self,
        mock_open_rasterio,
        *_,
    ):
        mock_open_rasterio.side_effect = [
            xr.Dataset({"a": ("x", [1])}),
            xr.Dataset({"a": ("x", [2])}),
        ]

        ds = self.cdse_handler.open_data(
            "daily-surface-soil-moisture-v1.0",
            time_range=("2024-01-01", "2024-01-02"),
        )

        self.assertIsInstance(ds, xr.Dataset)

    def test_request_download_not_implemented(self):
        self.assertIsNone(self.cdse_handler.request_download("any"))

    def test_prepare_request_not_implemented(self):
        self.assertIsNone(self.cdse_handler.prepare_request("any"))


class TestCdsePrivateHelpers(unittest.TestCase):
    def test_append_nc_file(self):
        path = "/some/path/file_nc"
        result = _append_nc_file(path)
        self.assertEqual(result, "/some/path/file_nc/file.nc")

    def test_get_min_max_date(self):
        df = pd.DataFrame(
            {
                "content_date_start": [
                    "2024-01-10",
                    "2024-01-01",
                    "2024-01-05",
                ]
            }
        )

        min_date, max_date = _get_min_max_date(df)

        self.assertEqual(min_date, datetime(2024, 1, 1))
        self.assertEqual(max_date, datetime(2024, 1, 10))

    @patch("xcube_clms.product_handlers.cdse._get_df_from_data_id")
    def test_generate_daily_ssm_paths_basic(self, mock_get_df):
        mock_get_df.return_value = pd.DataFrame(
            {
                "content_date_start": [
                    "2024-01-01",
                    "2024-01-02",
                    "2024-01-03",
                ],
                "s3_path": [
                    "/a/file1_nc",
                    "/a/file2_nc",
                    "/a/file3_nc",
                ],
            }
        )

        paths = _generate_daily_ssm_paths(
            "daily-surface-soil-moisture-v1.0",
            "2024-01-01",
            "2024-01-02",
        )

        self.assertEqual(
            paths,
            [
                "/a/file1_nc/file1.nc",
                "/a/file2_nc/file2.nc",
            ],
        )

    @patch("xcube_clms.product_handlers.cdse.LOG.warn")
    @patch("xcube_clms.product_handlers.cdse._get_df_from_data_id")
    def test_generate_daily_ssm_paths_warns_outside_range(self, mock_get_df, mock_warn):
        mock_get_df.return_value = pd.DataFrame(
            {
                "content_date_start": ["2024-01-05"],
                "s3_path": ["/a/file_nc"],
            }
        )

        paths = _generate_daily_ssm_paths(
            "daily-surface-soil-moisture-v1.0",
            "2024-01-01",
            "2024-01-10",
        )

        self.assertEqual(paths, ["/a/file_nc/file.nc"])

        self.assertEqual(mock_warn.call_count, 2)

    @patch("xcube_clms.product_handlers.cdse._get_df_from_data_id")
    def test_generate_daily_ssm_paths_no_match(self, mock_get_df):
        mock_get_df.return_value = pd.DataFrame(
            {
                "content_date_start": ["2023-01-01"],
                "s3_path": ["/a/file_nc"],
            }
        )

        paths = _generate_daily_ssm_paths(
            "daily-surface-soil-moisture-v1.0",
            "2024-01-01",
            "2024-01-02",
        )

        self.assertEqual(paths, [])
