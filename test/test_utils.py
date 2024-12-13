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
import threading
import time
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import MagicMock, patch

import pytest
from requests import Response, JSONDecodeError, RequestException, Timeout, \
    HTTPError

from xcube_clms.utils import (
    get_response_of_type,
    make_api_request,
    build_api_url,
    spinner,
)

url = "http://example.com/api"


@pytest.fixture
def mock_successful_response():
    mock_response = MagicMock(spec=Response)
    mock_response.status_code = 200
    mock_response.text = '{"key": "value"}'
    mock_response.json.return_value = {"key": "value"}
    mock_response.headers = {"Content-Type": "application/json"}
    return mock_response


@patch("requests.Session")
def test_make_api_request_success(mock_session):

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.status_code = 200
    mock_response.text = '{"key": "value"}'
    mock_response.json.return_value = {"key": "value"}

    mock_session.return_value.request.return_value = mock_response

    result = make_api_request(url)
    assert result.json() == {"key": "value"}
    mock_response.json.assert_called_once()


@patch("requests.Session")
def test_make_api_request_http_error(mock_session):
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.text = "Not Found"
    mock_response.headers = {"Content-Type": "application/text"}
    mock_response.raise_for_status.side_effect = HTTPError("HTTP error 404")
    mock_session.return_value.request.return_value = mock_response

    with pytest.raises(HTTPError, match="HTTP error 404"):
        make_api_request(url)

    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.text = "Bad Request"
    mock_response.headers = {"Content-Type": "application/json"}
    mock_response.raise_for_status.side_effect = HTTPError("HTTP error 400")
    mock_response.json.return_value = {"error": "Invalid request"}

    mock_session.return_value.request.return_value = mock_response

    with pytest.raises(
        HTTPError,
        match="HTTP error 400",
    ):
        make_api_request(url)


@patch("requests.Session")
def test_make_api_request_json_decode_error(mock_session):
    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.text = "Not a JSON response"
    mock_response.headers = {"Content-Type": "application/json"}
    mock_response.raise_for_status.side_effect = HTTPError("HTTP error 404")
    mock_response.json.side_effect = JSONDecodeError("Cannot decode", "", 0)

    mock_session.return_value.request.return_value = mock_response
    with pytest.raises(JSONDecodeError, match="Cannot decode"):
        make_api_request(url)


@patch("requests.Session")
def test_make_api_request_timeout_error(mock_session):
    mock_session.return_value.request.side_effect = Timeout("Request timed out")

    with pytest.raises(Timeout, match="Request timed out"):
        make_api_request(url)


@patch("requests.Session")
def test_make_api_request_request_exception(mock_session):
    mock_session.return_value.request.side_effect = RequestException("Connection error")

    with pytest.raises(RequestException, match="Connection error"):
        make_api_request(url)


@patch("requests.Session")
def test_make_api_request_unknown_exception(mock_session):
    mock_session.return_value.request.side_effect = Exception("Unknown error")

    with pytest.raises(Exception, match="Unknown error"):
        make_api_request(url)


def test_get_response_of_type_json(mock_successful_response):
    mock_successful_response.headers = {"Content-Type": "application/json"}
    mock_successful_response.json.return_value = {"key": "value"}

    result = get_response_of_type(mock_successful_response, "json")
    assert result == {"key": "value"}
    mock_successful_response.json.assert_called_once()


def test_get_response_of_type_text(mock_successful_response):
    mock_successful_response.headers = {"Content-Type": "text/html"}
    mock_successful_response.text = "This is some text content"

    result = get_response_of_type(mock_successful_response, "text")
    assert result == "This is some text content"
    mock_successful_response.text = "This is some text content"


def test_get_response_of_type_bytes(mock_successful_response):
    mock_successful_response.headers = {"Content-Type": "application/octet-stream"}
    mock_successful_response.content = (
        b"binary content"  # Mocking the content attribute
    )

    result = get_response_of_type(mock_successful_response, "bytes")
    assert result == b"binary content"
    mock_successful_response.content = b"binary content"


def test_get_response_of_type_invalid_response_type():
    with pytest.raises(
        TypeError, match="Invalid input: response_data must be a Response, got 'dict'."
    ):
        get_response_of_type({}, "json")


def test_get_response_of_type_invalid_data_type(mock_successful_response):
    mock_successful_response.headers = {"Content-Type": "application/json"}

    with pytest.raises(
        ValueError,
        match=r"Invalid data_type: xml. Must be one of \{'(json|text|bytes)', '(json|text|bytes)', '(json|text|bytes)'\}.",
    ):
        get_response_of_type(mock_successful_response, "xml")


def test_get_response_of_type_content_type_mismatch(mock_successful_response):
    mock_successful_response.headers = {"Content-Type": "application/json"}
    mock_successful_response.json.return_value = {"key": "value"}

    with pytest.raises(
        ValueError,
        match="Type mismatch: Expected text, but response is of type 'json'.",
    ):
        get_response_of_type(mock_successful_response, "text")

    mock_successful_response.headers = {"Content-Type": "text/html"}
    mock_successful_response.text = "This is some text content"

    with pytest.raises(
        ValueError,
        match="Type mismatch: Expected json, but response is of type 'text'.",
    ):
        get_response_of_type(mock_successful_response, "json")


def test_get_response_of_type_missing_content_type(mock_successful_response):
    mock_successful_response.headers = {}
    mock_successful_response.content = b"unknown content"

    result = get_response_of_type(mock_successful_response, "bytes")
    assert result == b"unknown content"
    assert mock_successful_response.content == b"unknown content"


def test_get_response_of_type_invalid_content_for_bytes(mock_successful_response):
    mock_successful_response.headers = {"Content-Type": "application/json"}
    mock_successful_response.json.return_value = {
        "data": "value"
    }  # Mocking the JSON response

    with pytest.raises(
        ValueError,
        match="Type mismatch: Expected bytes, but response is of type 'json'.",
    ):
        get_response_of_type(mock_successful_response, "bytes")


def test_build_api_url_no_parameters():
    api_endpoint = "data"
    expected_url = "http://example.com/api/data/?portal_type=DataSet" "&fullobjects=1"
    result = build_api_url(url, api_endpoint)
    assert result == expected_url


def test_build_api_url_with_metadata_fields():
    api_endpoint = "data"
    metadata_fields = ["field1", "field2"]
    expected_url = (
        "http://example.com/api/data/?portal_type=DataSet"
        "&fullobjects=1&metadata_fields=field1%2Cfield2"
    )
    result = build_api_url(url, api_endpoint, metadata_fields)
    assert result == expected_url


def test_build_api_url_with_datasets_request_false():
    api_endpoint = "data"
    expected_url = "http://example.com/api/data"
    result = build_api_url(url, api_endpoint, datasets_request=False)
    assert result == expected_url


def test_spinner():
    status_event = threading.Event()

    message = "Test task"
    output = StringIO()

    status_event.set()
    with redirect_stdout(output):
        spinner_thread = threading.Thread(target=spinner, args=(status_event, message))
        spinner_thread.start()
        time.sleep(1.2)
        status_event.clear()
        spinner_thread.join()

    spinner_output = output.getvalue()
    assert "Test task" in spinner_output
    assert "Elapsed time:" in spinner_output
    assert "Done!" in spinner_output
