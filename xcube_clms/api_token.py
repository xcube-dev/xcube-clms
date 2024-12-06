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


class CLMSAPIToken:
    def __init__(
        self,
        credentials: dict,
    ):
        self._credentials: dict = credentials
        self._token_expiry: int = 0
        self._token_lifetime: int = 3600  # Token lifetime in seconds
        self._expiry_margin: int = 300  # Refresh 5 minutes before expiration
        self.access_token: str = ""
        self.refresh_token()

    def refresh_token(self) -> str:
        try:
            self.access_token = self._request_access_token()
            self._token_expiry = time.time() + self._token_lifetime
            LOG.info("Token refreshed successfully.")
        except RequestException as e:
            LOG.error("Token refresh failed: ", e)
            raise e

        return self.access_token

    def _request_access_token(self) -> str:
        headers = ACCEPT_HEADER.copy()
        headers["Content-Type"] = "application/x-www-form-urlencoded"

        data = {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": self._create_JWT_grant(),
        }
        response_data = make_api_request(
            CLMS_API_AUTH, headers=headers, data=data, method="POST"
        )
        response = get_response_of_type(response_data, JSON_TYPE)

        return response["access_token"]

    def _create_JWT_grant(self):
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
        return time.time() > (self._token_expiry - self._expiry_margin)
