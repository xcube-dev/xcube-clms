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

from unittest.mock import patch, MagicMock

import pytest
from requests import RequestException

from xcube_clms.api_token_handler import ClmsApiTokenHandler

credentials = {
    "client_id": "test_client_id",
    "user_id": "test_user_id",
    "token_uri": "test_token_uri",
    "private_key": "test_private_key",
}


@pytest.fixture
def mock_jwt_encode():
    with patch("xcube_clms.api_token_handler.jwt.encode") as mock:
        yield mock


@pytest.fixture
def mock_make_api_request():
    with patch("xcube_clms.api_token_handler.make_api_request") as mock:
        yield mock


@pytest.fixture
def mock_get_response_of_type():
    with patch("xcube_clms.api_token_handler.get_response_of_type") as mock:
        yield mock


@pytest.fixture
def mock_time():
    with patch("time.time") as mock:
        yield mock


@pytest.fixture
def mock_log_info():
    with patch("xcube_clms.api_token_handler.LOG.info") as mock:
        yield mock


@pytest.fixture
def mock_log_error():
    with patch("xcube_clms.api_token_handler.LOG.error") as mock:
        yield mock


def test_create_jwt_grant(
    mock_jwt_encode,
    mock_make_api_request,
    mock_get_response_of_type,
    mock_time,
    mock_log_info,
):
    mock_jwt_encode.return_value = "mocked_jwt_token"
    mock_make_api_request.return_value = MagicMock(
        status_code=200, json={"access_token": "mocked_access_token"}
    )
    mock_get_response_of_type.return_value = {"access_token": "mocked_access_token"}
    mock_time.return_value = 1234567890

    token = ClmsApiTokenHandler(credentials)

    mock_jwt_encode.assert_called_once()
    mock_make_api_request.assert_called_once()
    assert token.api_token == "mocked_access_token"
    mock_time.assert_called()
    mock_log_info.assert_called_with("Token refreshed successfully.")


def test_is_token_expired(
    mock_jwt_encode, mock_make_api_request, mock_get_response_of_type, mock_time
):
    mock_jwt_encode.return_value = "mocked_jwt_token"
    mock_make_api_request.return_value = MagicMock(
        status_code=200, json={"access_token": "mocked_access_token"}
    )
    mock_get_response_of_type.return_value = {"access_token": "mocked_access_token"}
    mock_time.return_value = 1234567890

    token = ClmsApiTokenHandler(credentials)
    token._token_expiry = 1234567800
    assert token.is_token_expired() is True

    token = ClmsApiTokenHandler(credentials)
    token._token_expiry = 1234569900
    assert token.is_token_expired() is False


def test_refresh_token(
    mock_jwt_encode, mock_make_api_request, mock_get_response_of_type, mock_log_info
):
    mock_jwt_encode.return_value = "mocked_jwt_token"
    mock_make_api_request.return_value = MagicMock(
        status_code=200, json={"access_token": "mocked_access_token"}
    )
    mock_get_response_of_type.return_value = {"access_token": "mocked_access_token"}

    handler = ClmsApiTokenHandler(credentials)

    assert handler.api_token == "mocked_access_token"
    mock_log_info.assert_called_with("Token refreshed successfully.")


def test_refresh_token_failure(mock_jwt_encode, mock_make_api_request, mock_log_error):
    mock_make_api_request.side_effect = RequestException("Mocked request failure")
    with pytest.raises(RequestException, match="Mocked request failure"):
        ClmsApiTokenHandler(credentials)
    mock_log_error.assert_called_with(
        "Token refresh failed: ", mock_make_api_request.side_effect
    )


@patch("xcube_clms.api_token_handler.ClmsApiTokenHandler.is_token_expired")
@patch("xcube_clms.api_token_handler.LOG")
def test_refresh_token_expired(
    mock_log,
    mock_expiry,
    mock_time,
    mock_jwt_encode,
    mock_make_api_request,
    mock_get_response_of_type,
):
    mock_jwt_encode.return_value = "mocked_jwt_token"
    mock_make_api_request.return_value = MagicMock(
        status_code=200, json={"access_token": "mocked_access_token"}
    )
    mock_get_response_of_type.return_value = {"access_token": "mocked_access_token"}

    mock_time.return_value = 1000

    handler = ClmsApiTokenHandler(credentials)
    assert handler.api_token == "mocked_access_token"
    assert handler._token_expiry == 3600 + 1000

    mock_time.reset_mock()
    mock_time.return_value = 3900

    mock_expiry.return_value = True
    mock_get_response_of_type.return_value = {"access_token": "new_mocked_access_token"}

    handler.refresh_token()
    assert handler.is_token_expired() is True
    mock_log.info.assert_any_call("Token expired or not present. Refreshing token.")
    assert handler.api_token == "new_mocked_access_token"
