import unittest
from unittest.mock import MagicMock, patch

from xcube.core.store import DataStoreError

from xcube_clms.constants import ID_KEY, UID_KEY
from xcube_clms.product_handlers.legacy import (
    _CHARACTERISTICS_TEMPORAL_EXTENT,
    LegacyProductHandler,
)


class TestLegacyProductHandler(unittest.TestCase):
    def setUp(self):
        self.mock_api_token_handler = MagicMock()
        self.mock_api_token_handler.api_token = "mock_token"

        self.mock_datasets_info = [
            {
                "id": "mock_uid_mock_id",
                "UID": "mock_uid",
                "dataset_download_information": {
                    "items": [
                        {"@id": "mock_id", "path": "mock_path", "full_source": "EEA"}
                    ]
                },
            },
            {
                "id": "mock_uid_legacy_mock_id",
                "UID": "mock_uid",
                "dataset_download_information": {
                    "items": [
                        {"@id": "mock_id", "path": "mock_path", "full_source": "LEGACY"}
                    ]
                },
                "characteristics_temporal_extent": "2024-2024",
            },
            {
                "id": "mock_uid_legacy_present_mock_id",
                "UID": "mock_uid",
                "dataset_download_information": {
                    "items": [
                        {"@id": "mock_id", "path": "mock_path", "full_source": "LEGACY"}
                    ]
                },
                "characteristics_temporal_extent": "2024-present",
            },
            {
                "id": "dataset_id",
                "UID": "dataset_id",
                "dataset_download_information": {
                    "items": [
                        {"@id": "file_id", "path": "mock_path", "full_source": "EEA"}
                    ]
                },
                "downloadable_files": {
                    "items": [{"file": "file_id", "@id": "file_@id"}]
                },
            },
        ]

        self.legacy_handler = LegacyProductHandler(
            cache_store=None,
            datasets_info=self.mock_datasets_info,
            api_token_handler=self.mock_api_token_handler,
        )

    def test_has_data(self):
        self.assertTrue(self.legacy_handler.has_data("dataset_id"))
        self.assertFalse(self.legacy_handler.has_data("unknown_id"))

    def test_product_type(self):
        self.assertEqual("legacy", self.legacy_handler.product_type())

    def test_get_open_data_params_schema(self):
        schema = self.legacy_handler.get_open_data_params_schema()
        self.assertIn("time_range", schema.properties)

    @patch("xcube_clms.product_handlers.legacy.get_response_of_type")
    @patch("xcube_clms.product_handlers.legacy.make_api_request")
    @patch.object(LegacyProductHandler, "prepare_request")
    def test_request_download(
        self, mock_prepare_request, mock_make_api_request, mock_get_response
    ):
        mock_prepare_request.return_value = (
            "http://mock.url",
            {"Authorization": "Bearer xyz"},
        )
        mock_get_response.return_value = ["url1", "url2"]
        mock_make_api_request.return_value = {"response": "data"}

        result = self.legacy_handler.request_download("data123")

        self.mock_api_token_handler.refresh_token.assert_called_once()
        self.assertEqual(result, ["url1", "url2"])

    @patch("xcube_clms.product_handlers.legacy.get_extracted_component")
    @patch("xcube_clms.product_handlers.legacy.build_api_url")
    @patch("xcube_clms.product_handlers.legacy.get_authorization_header")
    def test_prepare_request(self, mock_get_auth, mock_build_url, mock_get_component):
        mock_get_component.side_effect = [
            {ID_KEY: "item-123"},
            {
                UID_KEY: "uid-456",
                _CHARACTERISTICS_TEMPORAL_EXTENT: "2020-2022",
            },
        ]
        mock_build_url.return_value = "http://download.url"
        mock_get_auth.return_value = {"Authorization": "Bearer xyz"}

        url, headers = self.legacy_handler.prepare_request("data123")
        self.assertEqual(url, "http://download.url")
        self.assertIn("Authorization", headers)

    @patch.object(LegacyProductHandler, "request_download")
    def test_filter_urls_no_filter(self, mock_request_download):
        mock_request_download.return_value = ["url1", "url2"]
        result = self.legacy_handler.filter_urls("id123")
        self.assertEqual(result, ["url1", "url2"])

    @patch("xcube_clms.product_handlers.legacy.extract_and_filter_dates")
    @patch.object(LegacyProductHandler, "request_download")
    def test_filter_urls_with_filter(self, mock_request_download, mock_filter_dates):
        mock_request_download.return_value = ["url1", "url2"]
        mock_filter_dates.return_value = ["url1"]
        result = self.legacy_handler.filter_urls(
            "id123", time_range=("2020-01-01", "2021-01-01")
        )
        self.assertEqual(result, ["url1"])

    @patch("xcube_clms.product_handlers.legacy.extract_and_filter_dates")
    @patch.object(LegacyProductHandler, "request_download")
    def test_filter_urls_with_filter_no_results(
        self, mock_request_download, mock_filter_dates
    ):
        mock_request_download.return_value = ["url1", "url2"]
        mock_filter_dates.return_value = []
        with self.assertRaises(DataStoreError):
            self.legacy_handler.filter_urls(
                "id123", time_range=("2020-01-01", "2021-01-01")
            )

    @patch.object(LegacyProductHandler, "filter_urls")
    @patch("xcube_clms.product_handlers.legacy.xr.open_mfdataset")
    def test_open_data_netcdf(self, mock_open_mfdataset, mock_filter_urls):
        mock_filter_urls.return_value = ["netcdf_url.nc"]
        self.legacy_handler.open_data("id123")
        mock_open_mfdataset.assert_called_once_with(
            ["netcdf_url.nc"], engine="h5netcdf"
        )

    @patch.object(LegacyProductHandler, "filter_urls")
    @patch("xcube_clms.product_handlers.legacy.xr.open_mfdataset")
    def test_open_data_geotiff(self, mock_open_mfdataset, mock_filter_urls):
        mock_filter_urls.return_value = ["geotiff_uri.tif"]
        self.legacy_handler.open_data("id123")
        mock_open_mfdataset.assert_called_with(["geotiff_uri.tif"], engine="rasterio")

        mock_filter_urls.return_value = ["geotiff_uri.tiff"]
        self.legacy_handler.open_data("id123")
        mock_open_mfdataset.assert_called_with(["geotiff_uri.tiff"], engine="rasterio")

    @patch.object(LegacyProductHandler, "filter_urls")
    def test_open_data_invalid_format(self, mock_filter_urls):
        mock_filter_urls.return_value = ["unknown_url.hdf"]
        with self.assertRaises(DataStoreError):
            self.legacy_handler.open_data("id123")
