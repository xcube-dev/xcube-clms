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
from unittest.mock import MagicMock, patch

from requests import Response, JSONDecodeError, RequestException, Timeout, \
    HTTPError

from xcube_clms.utils import (
    get_response_of_type,
    make_api_request,
    build_api_url,
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

    def test_build_api_url_no_parameters(self):
        api_endpoint = "data"
        expected_url = (
            "http://example.com/api/data/?portal_type=DataSet" "&fullobjects=1"
        )
        result = build_api_url(url, api_endpoint)
        self.assertEqual(result, expected_url)

    def test_build_api_url_with_metadata_fields(self):
        api_endpoint = "data"
        metadata_fields = ["field1", "field2"]
        expected_url = (
            "http://example.com/api/data/?portal_type=DataSet"
            "&fullobjects=1&metadata_fields=field1%2Cfield2"
        )
        result = build_api_url(url, api_endpoint, metadata_fields)
        self.assertEqual(result, expected_url)

    def test_build_api_url_with_datasets_request_false(self):
        api_endpoint = "data"
        expected_url = "http://example.com/api/data"
        result = build_api_url(url, api_endpoint, datasets_request=False)
        self.assertEqual(result, expected_url)
