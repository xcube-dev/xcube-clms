from unittest.mock import patch, MagicMock

import pytest
from requests import RequestException

from xcube_clms.api_token import CLMSAPIToken

credentials = {
    "client_id": "test_client_id",
    "user_id": "test_user_id",
    "token_uri": "test_token_uri",
    "private_key": "test_private_key",
}


@pytest.fixture
def mock_jwt_encode():
    with patch("xcube_clms.api_token.jwt.encode") as mock:
        yield mock


@pytest.fixture
def mock_make_api_request():
    with patch("xcube_clms.api_token.make_api_request") as mock:
        yield mock


@pytest.fixture
def mock_get_response_of_type():
    with patch("xcube_clms.api_token.get_response_of_type") as mock:
        yield mock


@pytest.fixture
def mock_time():
    with patch("time.time") as mock:
        yield mock


@pytest.fixture
def mock_log_info():
    with patch("xcube_clms.api_token.LOG.info") as mock:
        yield mock


@pytest.fixture
def mock_log_error():
    with patch("xcube_clms.api_token.LOG.error") as mock:
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

    token = CLMSAPIToken(credentials)

    mock_jwt_encode.assert_called_once()
    mock_make_api_request.assert_called_once()
    assert token.access_token == "mocked_access_token"
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

    token = CLMSAPIToken(credentials)
    token._token_expiry = 1234567800
    assert token.is_token_expired() is True

    token = CLMSAPIToken(credentials)
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

    token = CLMSAPIToken(credentials)

    assert token.access_token == "mocked_access_token"
    mock_log_info.assert_called_with("Token refreshed successfully.")


def test_refresh_token_failure(mock_jwt_encode, mock_make_api_request, mock_log_error):
    mock_make_api_request.side_effect = RequestException("Mocked request failure")
    with pytest.raises(RequestException, match="Mocked request failure"):
        CLMSAPIToken(credentials)
    mock_log_error.assert_called_with(
        "Token refresh failed: ", mock_make_api_request.side_effect
    )
