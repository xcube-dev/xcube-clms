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

import time

import jwt
from requests import RequestException

from xcube_clms.constants import ACCEPT_HEADER, CLMS_API_AUTH, JSON_TYPE, LOG
from xcube_clms.utils import make_api_request, get_response_of_type


class ClmsApiTokenHandler:
    """
    Manages the OAuth2 access token for authenticating with the CLMS API.
    This class is responsible for refreshing the token when it expires,
    generating a JWT (JSON Web Token) grant, and checking if the current
    token is expired.
    """

    def __init__(self, credentials: dict[str, str]) -> None:
        """
        Initializes the CLMSAPIToken object with the given credentials and sets
        up the necessary values for token management.

        Args:
            credentials: A dictionary containing the credentials
        """
        self._credentials: dict = credentials
        self._token_expiry: int = 0
        self._token_lifetime: int = 3600  # Token lifetime in seconds
        self._expiry_margin: int = 300  # Refresh 5 minutes before expiration
        self.api_token: str = ""
        self.refresh_token()

    def refresh_token(self) -> str:
        """
        Refreshes the access token by requesting a new one from the CLMS API.
        Updates the token expiry time after a successful refresh.
        This method checks the current token's status and either refreshes it
        or logs that the existing token is still valid.

        Returns:
            str: The new access token.

        Raises:
            RequestException: If the token refresh fails.
        """
        if not self.api_token or self.is_token_expired():
            LOG.info("Token expired or not present. Refreshing token.")
            try:
                self.api_token = self._request_access_token()
                self._token_expiry = time.time() + self._token_lifetime
                LOG.info("Token refreshed successfully.")
            except RequestException as e:
                LOG.error("Token refresh failed: ", e)
                raise e
        else:
            LOG.info("Current token valid. Reusing it.")

        return self.api_token

    def _request_access_token(self) -> str:
        """Make an API request to obtain a new access token using the JWT grant.

        Returns:
            str: The access token from the response.

        Raises:
            RequestException: If the API request fails.
        """
        headers = ACCEPT_HEADER.copy()
        headers["Content-Type"] = "application/x-www-form-urlencoded"

        data = {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": self._create_jwt_grant(),
        }
        response_data = make_api_request(
            CLMS_API_AUTH, headers=headers, data=data, method="POST"
        )
        response = get_response_of_type(response_data, JSON_TYPE)

        return response["access_token"]

    def _create_jwt_grant(self) -> str:
        """
        Creates an encoded JWT used to obtain the access token.

        Returns:
            str: The JWT grant as a string.
        """
        private_key = self._credentials["private_key"].encode("utf-8")

        claim_set = {
            "iss": self._credentials["client_id"],
            "sub": self._credentials["user_id"],
            "aud": self._credentials["token_uri"],
            "iat": int(time.time()),
            "exp": int(time.time() + self._token_lifetime),
        }
        return jwt.encode(claim_set, private_key, algorithm="RS256")

    def is_token_expired(self) -> bool:
        """
        Checks if the current access token has expired.

        Returns:
            bool: True if the token has expired, False otherwise.
        """
        return time.time() > (self._token_expiry - self._expiry_margin)
