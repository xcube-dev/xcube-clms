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

from xcube_clms.api_token import CLMSAPIToken
from xcube_clms.constants import LOG


class TokenHandler:
    """
    Handles the management and refreshing of API tokens for the CLMS API.

    Attributes:
        _credentials: Authentication credentials used for generating tokens.
        _clms_api_token_instance: Instance for handling token-related operations.
        api_token: The current API token for authenticated requests.
    """

    def __init__(self, credentials: dict) -> None:
        """
        Initialize the TokenHandler with the given credentials.

        Args:
            credentials: A dictionary containing authentication details.
        """
        self._credentials = credentials
        self._clms_api_token_instance = CLMSAPIToken(credentials=credentials)
        self.api_token: str = self._clms_api_token_instance.access_token

    def refresh_token(self) -> None:
        """
        Refreshes the API token if it is expired or not present.

        This method checks the current token's status and either refreshes it
        or logs that the existing token is still valid.
        """
        if not self.api_token or self._clms_api_token_instance.is_token_expired():
            LOG.info("Token expired or not present. Refreshing token.")
            self.api_token = self._clms_api_token_instance.refresh_token()
        else:
            LOG.info("Current token valid. Reusing it.")
