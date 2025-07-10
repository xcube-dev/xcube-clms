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
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch, Mock

import fsspec
import pytest
import xarray as xr
from requests import HTTPError
from requests import JSONDecodeError
from requests import RequestException
from requests import Response
from requests import Timeout
from xcube.core.store import DataStoreError

from xcube_clms.utils import (
    get_response_of_type,
    make_api_request,
    build_api_url,
    get_spatial_dims,
    cleanup_dir,
    extract_and_filter_dates,
    download_zip_data,
)

url = "http://example.com/api"


def setup_requests_mock(
    mock_session: MagicMock(),
    status_code: int = 200,
    text: str = None,
    json_data: dict | None = None,
    headers: dict[str, str] | None = None,
    raise_for_status_side_effect: Exception | None = None,
):
    mock_response = MagicMock()
    mock_response.status_code = status_code
    mock_response.text = text
    mock_response.json.return_value = json_data
    mock_response.headers = headers or {}
    mock_response.raise_for_status.side_effect = (
        None
        if (200 <= status_code < 300 or not raise_for_status_side_effect)
        else raise_for_status_side_effect
    )
    mock_session.return_value.request.return_value = mock_response

    return mock_session


class UtilsTest(unittest.TestCase):
    @patch("requests.Session")
    def test_make_api_request_success(self, mock_session):
        setup_requests_mock(mock_session, json_data={"key": "value"})
        result = make_api_request(url)
        self.assertEqual(result.json(), {"key": "value"})

    @patch("requests.Session")
    def test_make_api_request_http_error(self, mock_session):
        setup_requests_mock(
            mock_session,
            status_code=400,
            text="Bad Request",
            json_data={"error": "Invalid request"},
            headers={"Content-Type": "application/json"},
            raise_for_status_side_effect=HTTPError("Mocked HTTP Error"),
        )
        with self.assertRaisesRegex(
            HTTPError,
            r"HTTP error 400: \{'error': 'Invalid request'\}. Original error: Mocked HTTP Error",
        ):
            make_api_request(url)

        setup_requests_mock(
            mock_session,
            status_code=404,
            text="Not found",
            headers={"Content-Type": "application/text"},
            raise_for_status_side_effect=HTTPError("Mocked HTTP Error"),
        )
        with self.assertRaisesRegex(
            HTTPError, r"HTTP error 404: Not found. Original error: Mocked HTTP Error"
        ):
            make_api_request(url)

    @patch("requests.Session")
    def test_make_api_request_json_decode_error_raises_http_error_with_orig_error(
        self,
        mock_session,
    ):
        mock_session = setup_requests_mock(
            mock_session,
            status_code=400,
            text="Not a JSON response",
            headers={"Content-Type": "application/json"},
            raise_for_status_side_effect=HTTPError("Mocked HTTP Error"),
        )
        mock_session.return_value.request.return_value.json.side_effect = (
            JSONDecodeError("", "", 0)
        )
        with self.assertRaisesRegex(
            HTTPError,
            r"HTTP error 400: Not a JSON response. Original error: Mocked HTTP Error",
        ):
            make_api_request(url)

    @patch("requests.Session")
    def test_make_api_request_timeout_error(self, mock_session):
        mock_session.return_value.request.side_effect = Timeout("Request timed out")

        with self.assertRaisesRegex(Timeout, "Request timed out"):
            make_api_request(url)

    @patch("requests.Session")
    def test_make_api_request_request_exception(self, mock_session):
        mock_session.return_value.request.side_effect = RequestException(
            "Connection error"
        )
        with self.assertRaisesRegex(RequestException, "Connection error"):
            make_api_request(url)

    @patch("requests.Session")
    def test_make_api_request_unknown_exception(self, mock_session):
        mock_session.return_value.request.side_effect = Exception("Unknown error")
        with self.assertRaisesRegex(Exception, "Unknown error"):
            make_api_request(url)

    def test_get_response_of_type_json(self):
        mock_response = MagicMock(spec=Response)
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {"key": "value"}

        result = get_response_of_type(mock_response, "json")
        self.assertEqual(result, {"key": "value"})

    def test_get_response_of_type_text(self):
        mock_response = MagicMock(spec=Response)
        mock_response.headers = {"Content-Type": "application/text"}
        mock_response.text = "This is some text content"

        result = get_response_of_type(mock_response, "text")
        self.assertEqual(result, "This is some text content")

    def test_get_response_of_type_bytes(self):
        mock_response = MagicMock(spec=Response)
        mock_response.headers = {}
        mock_response.content = b"binary content"

        result = get_response_of_type(mock_response, "bytes")
        self.assertEqual(result, b"binary content")

    def test_get_response_of_type_invalid_response_type(self):
        with self.assertRaisesRegex(
            TypeError, "Invalid input: response_data must be a Response, got 'dict'."
        ):
            get_response_of_type({}, "json")

    def test_get_response_of_type_invalid_data_type(self):
        mock_response = MagicMock(spec=Response)
        mock_response.headers = {"Content-Type": "application/json"}
        with self.assertRaisesRegex(
            ValueError,
            r"Invalid data_type: xml. Must be one of \{'(json|text|bytes)', '(json|text|bytes)', '(json|text|bytes)'\}.",
        ):
            get_response_of_type(mock_response, "xml")

    def test_get_response_of_type_content_type_mismatch(self):
        mock_response = MagicMock(spec=Response)
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {"key": "value"}
        with self.assertRaisesRegex(
            ValueError, "Type mismatch: Expected text, but response is of type 'json'."
        ):
            get_response_of_type(mock_response, "text")

        mock_response.headers = {"Content-Type": "text/html"}
        mock_response.text = "This is some text content"

        with self.assertRaisesRegex(
            ValueError, "Type mismatch: Expected json, but response is of type 'text'."
        ):
            get_response_of_type(mock_response, "json")

    def test_get_response_of_type_missing_content_type(self):
        mock_response = MagicMock(spec=Response)
        mock_response.headers = {}
        mock_response.content = b"unknown content"

        result = get_response_of_type(mock_response, "bytes")
        self.assertEqual(result, b"unknown content")

    def test_get_response_of_type_invalid_content_for_bytes(self):
        mock_response = MagicMock(spec=Response)
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {"data": "value"}  # Mocking the JSON response

        with self.assertRaisesRegex(
            ValueError, "Type mismatch: Expected bytes, but response is of type 'json'."
        ):
            get_response_of_type(mock_response, "bytes")

    def test_build_api_url_no_query_parameters(self):
        api_endpoint = "data"
        expected_url = "http://example.com/api/data"
        result = build_api_url(url, api_endpoint)
        self.assertEqual(result, expected_url)

    def test_build_api_url_with_dataset_parameters(self):
        api_endpoint = "data"
        expected_url = "http://example.com/api/data/?portal_type=DataSet&fullobjects=1"
        result = build_api_url(url, api_endpoint, datasets_request=True)
        self.assertEqual(result, expected_url)

    def test_build_api_url_with_metadata_fields(self):
        api_endpoint = "data"
        metadata_fields = {"metadata_fields": "field1,field2"}
        expected_url = "http://example.com/api/data/?metadata_fields=field1%2Cfield2"
        result = build_api_url(url, api_endpoint, metadata_fields)
        self.assertEqual(result, expected_url)

    def test_build_api_url_with_datasets_request_false(self):
        api_endpoint = "data"
        expected_url = "http://example.com/api/data"
        result = build_api_url(url, api_endpoint)
        self.assertEqual(result, expected_url)

    def test_get_spatial_dims_lat_lon(self):
        ds = xr.Dataset(coords={"lat": [0, 1], "lon": [10, 20]})
        y, x = get_spatial_dims(ds)
        assert (y, x) == ("lat", "lon")

    def test_get_spatial_dims_y_x(self):
        ds = xr.Dataset(coords={"y": [0, 1], "x": [10, 20]})
        y, x = get_spatial_dims(ds)
        assert (y, x) == ("y", "x")

    def test_get_spatial_dims_invalid(self):
        ds = xr.Dataset(coords={"row": [0, 1], "col": [10, 20]})
        with pytest.raises(DataStoreError):
            get_spatial_dims(ds)

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

    @patch("xcube_clms.utils.fsspec.filesystem")
    @patch("xcube_clms.utils.make_api_request")
    @patch("tempfile.NamedTemporaryFile")
    @patch("builtins.open")
    def test_download_zip_data(
        self, mock_dest_open, mock_temp_file, mock_make_api_request, mock_fsspec
    ):
        mock_temp_file.return_value.__enter__.return_value.name = "/tmp/test"

        mock = Mock()
        mock.iter_content.return_value = [b"chunk1", b"chunk2"]
        mock_make_api_request.return_value = mock

        mock_file_fs = Mock(spec=fsspec.AbstractFileSystem)

        mock_outer_zip_fs = Mock()
        mock_outer_zip_fs.ls.return_value = [
            {
                "filename": "test.zip",
                "name": "test.zip",
            }
        ]

        mock_inner_zip_fs = Mock()
        mock_inner_zip_fs.ls.return_value = [{"name": "test.tif"}]
        mock_inner_zip_fs.isdir.return_value = False

        mock_fsspec.side_effect = lambda protocol, fo=None: {
            "zip": mock_outer_zip_fs if fo == "/tmp/test" else mock_inner_zip_fs,
            "file": mock_file_fs,
        }[protocol]

        outer_file = Mock()
        outer_context = Mock()
        outer_context.__enter__ = Mock(return_value=outer_file)
        outer_context.__exit__ = Mock()
        mock_outer_zip_fs.open.return_value = outer_context

        mock_source_file = Mock()
        mock_source_file.read.side_effect = [b"data", b""]
        inner_context = Mock()
        inner_context.__enter__ = Mock(return_value=mock_source_file)
        inner_context.__exit__ = Mock()
        mock_inner_zip_fs.open.return_value = inner_context

        mock_cache_store = MagicMock()

        download_zip_data(mock_cache_store, "http://test.com", "test_id")

        mock_make_api_request.assert_called_once_with(
            "http://test.com",
            timeout=600,
            stream=True,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://source-website.com",
            },
        )
        self.assertTrue(mock_cache_store.fs.makedirs.called)
        self.assertTrue(mock_outer_zip_fs.ls.called)
        self.assertTrue(mock_outer_zip_fs.open.called)
        self.assertTrue(mock_inner_zip_fs.ls.called)
        self.assertTrue(mock_inner_zip_fs.open.called)
        self.assertTrue(mock_dest_open.called)

    def test_extract_and_filter_dates_basic(self):
        urls = [
            "https://example.com/20220101/data.tif",
            "https://example.com/20220215/data.tif",
            "https://example.com/20220301/data.tif",
            "https://example.com/invalid/data.tif",  # No date
        ]
        time_range = ("2022-01-15", "2022-02-28")
        expected = [
            "https://example.com/20220215/data.tif",
        ]
        result = extract_and_filter_dates(urls, time_range)
        assert result == expected

    def test_extract_and_filter_dates_multiple_matches(self):
        urls = [
            "https://x/20220101/a.nc",
            "https://x/20220115/b.nc",
            "https://x/20220130/c.nc",
        ]
        time_range = ("2022-01-01", "2022-01-31")
        expected = sorted(urls)
        result = extract_and_filter_dates(urls, time_range)
        assert result == expected

    def test_extract_and_filter_dates_none_in_range(self):
        urls = [
            "https://x/20210101/a.tif",
            "https://x/20210315/b.tif",
        ]
        time_range = ("2022-01-01", "2022-12-31")
        result = extract_and_filter_dates(urls, time_range)
        assert result == []
