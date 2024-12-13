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
import unittest
from unittest.mock import patch

from requests import RequestException

from xcube_clms.api_token_handler import ClmsApiTokenHandler


class ClmsApiTokenHandlerTest(unittest.TestCase):
    def setUp(self):
        self.mock_jwt_encode_patcher = patch("xcube_clms.api_token_handler.jwt.encode")
        self.mock_make_api_request_patcher = patch(
            "xcube_clms.api_token_handler.make_api_request"
        )
        self.mock_get_response_of_type_patcher = patch(
            "xcube_clms.api_token_handler.get_response_of_type"
        )
        self.mock_time_patcher = patch("time.time")
        self.mock_log_info_patcher = patch("xcube_clms.api_token_handler.LOG.info")
        self.mock_log_error_patcher = patch("xcube_clms.api_token_handler.LOG.error")

        self.mock_jwt_encode = self.mock_jwt_encode_patcher.start()
        self.mock_make_api_request = self.mock_make_api_request_patcher.start()
        self.mock_get_response_of_type = self.mock_get_response_of_type_patcher.start()
        self.mock_time = self.mock_time_patcher.start()
        self.mock_log_info = self.mock_log_info_patcher.start()
        self.mock_log_error = self.mock_log_error_patcher.start()

        self.credentials = {
            "private_key": "mock_private_key",
            "client_id": "mock_client_id",
            "user_id": "mock_user_id",
            "token_uri": "mock_token_uri",
        }

    def tearDown(self):
        patch.stopall()

    def test_create_jwt_grant(self):
        self.mock_log_info.reset_mock()
        self.mock_get_response_of_type.return_value = {
            "access_token": "mocked_access_token"
        }

        token_handler = ClmsApiTokenHandler(self.credentials)

        self.assertEqual(token_handler.api_token, "mocked_access_token")
        self.mock_log_info.assert_called()

    def test_is_token_expired(self):
        self.mock_time.return_value = 1234567890

        token_handler = ClmsApiTokenHandler(self.credentials)
        token_handler._token_expiry = 1234567800  # Simulating an expired token
        self.assertTrue(token_handler.is_token_expired())

        token_handler._token_expiry = 1234569900  # Simulating a valid token
        self.assertFalse(token_handler.is_token_expired())

    def test_refresh_token(self):
        self.mock_log_info.reset_mock()
        self.mock_get_response_of_type.return_value = {
            "access_token": "mocked_access_token"
        }

        # refresh_token() is called from the init
        token_handler = ClmsApiTokenHandler(self.credentials)

        self.assertEqual(token_handler.api_token, "mocked_access_token")
        self.mock_log_info.assert_called()

    def test_refresh_token_failure(self):
        self.mock_log_error.reset_mock()
        self.mock_make_api_request.side_effect = RequestException(
            "Mocked request failure"
        )

        with self.assertRaises(RequestException, msg="Mocked request failure"):
            ClmsApiTokenHandler(self.credentials)

    @patch("xcube_clms.api_token_handler.ClmsApiTokenHandler.is_token_expired")
    def test_refresh_token_expired(self, mock_is_token_expired):
        self.mock_log_info.reset_mock()
        self.mock_get_response_of_type.return_value = {
            "access_token": "mocked_access_token"
        }
        self.mock_time.return_value = 1000

        token_handler = ClmsApiTokenHandler(self.credentials)
        self.assertEqual(token_handler._token_expiry, 3600 + 1000)

        mock_is_token_expired.return_value = True
        self.mock_get_response_of_type.return_value = {
            "access_token": "new_mocked_access_token"
        }

        token_handler.refresh_token()

        self.assertTrue(token_handler.is_token_expired())
        self.mock_log_info.assert_called()
        self.assertEqual(token_handler.api_token, "new_mocked_access_token")
