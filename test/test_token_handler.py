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
from unittest.mock import patch, MagicMock

from xcube_clms.token_handler import TokenHandler


class TestTokenHandler(unittest.TestCase):
    def __init__(self, methodName: str = "runTest"):
        super().__init__(methodName)
        self.credentials = {
            "client_id": "test_client_id",
            "user_id": "test_user_id",
            "token_uri": "test_token_uri",
            "private_key": "test_private_key",
        }

    @patch("xcube_clms.token_handler.CLMSAPIToken")
    def test_token_handler_initialization(self, mock_clms_api_token):
        mock_token_instance = MagicMock()
        mock_token_instance.access_token = "mocked_access_token"
        mock_clms_api_token.return_value = mock_token_instance

        handler = TokenHandler(self.credentials)

        mock_clms_api_token.assert_called_once_with(credentials=self.credentials)
        assert handler.api_token == "mocked_access_token"

    @patch("xcube_clms.token_handler.CLMSAPIToken")
    @patch("xcube_clms.token_handler.LOG")
    def test_refresh_token_expired(self, mock_log, mock_clms_api_token):
        mock_token_instance = MagicMock()
        mock_token_instance.access_token = "mocked_access_token"
        mock_token_instance.is_token_expired.return_value = True
        mock_token_instance.refresh_token.return_value = "new_mock_access_token"
        mock_clms_api_token.return_value = mock_token_instance

        handler = TokenHandler(self.credentials)
        assert handler.api_token == "mocked_access_token"

        handler.refresh_token()

        mock_token_instance.is_token_expired.assert_called_once()
        mock_token_instance.refresh_token.assert_called_once()
        mock_log.info.assert_any_call("Token expired or not present. Refreshing token.")
        assert handler.api_token == "new_mock_access_token"

    @patch("xcube_clms.token_handler.CLMSAPIToken")
    @patch("xcube_clms.token_handler.LOG")
    def test_refresh_token_valid(self, mock_log, mock_clms_api_token):
        mock_token_instance = MagicMock()
        mock_token_instance.access_token = "mocked_access_token"
        mock_token_instance.is_token_expired.return_value = False
        mock_clms_api_token.return_value = mock_token_instance

        handler = TokenHandler(self.credentials)
        assert handler.api_token == "mocked_access_token"

        handler.refresh_token()

        mock_token_instance.is_token_expired.assert_called_once()
        mock_token_instance.refresh_token.assert_not_called()
        mock_log.info.assert_any_call("Current token valid. Reusing it.")
        assert handler.api_token == "mocked_access_token"
