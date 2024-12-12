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
from itertools import cycle
from typing import Optional, Any, Union, Literal
from urllib.parse import urlencode

import requests
from requests import HTTPError, Timeout, RequestException, Response, JSONDecodeError
from xcube.core.store import DataTypeLike, DataStoreError, DATASET_TYPE

from xcube_clms.constants import (
    ACCEPT_HEADER,
    LOG,
)

_PORTAL_TYPE = {"portal_type": "DataSet"}
_METADATA_FIELDS = "metadata_fields"
_FULL_SCHEMA = "fullobjects"

# Using the auxiliary functions below from xcube-stac


def assert_valid_data_type(data_type: DataTypeLike):
    """Auxiliary function to assert if data type is supported
    by the store.

    Args:
        data_type: Data type that is to be checked.

    Raises:
        DataStoreError: Error, if *data_type* is not
            supported by the store.
    """
    if not is_valid_data_type(data_type):
        raise DataStoreError(
            f"Data type must be {DATASET_TYPE.alias!r} or but got {data_type!r}."
        )


def is_valid_data_type(data_type: DataTypeLike) -> bool:
    """Auxiliary function to check if data type is supported
    by the store.

    Args:
        data_type: Data type that is to be checked.

    Returns:
        True if *data_type* is supported by the store, otherwise False
    """
    return data_type is None or DATASET_TYPE.is_super_type_of(data_type)


ResponseType = Literal["json", "text", "bytes"]


def make_api_request(
    url: str,
    headers: Optional[dict[str, str]] = ACCEPT_HEADER,
    data: Optional[dict[str, Any]] = None,
    json: Optional[dict[str, Any]] = None,
    method: str = "GET",
    stream: bool = False,
    timeout: int = 100,
    show_spinner: bool = True,
) -> Response:
    """Makes an API request with custom configurations.

    Args:
        url: The URL to which the request will be sent.
        headers: A dictionary of HTTP headers to include in the request.
            Defaults to `ACCEPT_HEADER`.
        data: A dictionary of form data to send in the body of the request.
            Defaults to `None`.
        json: A dictionary representing a JSON payload to send in the request.
            Defaults to `None`.
        method: The HTTP method to use (e.g., "GET", "POST", "PUT", "DELETE").
        Defaults to "GET".
        stream: Whether to stream the response content. Defaults to `False`.
        timeout: The maximum time (in seconds) to wait for a response.
        Defaults to `100`.
        show_spinner: Whether to display a spinner to indicate activity.
        Defaults to `True`.

    Returns:
        Response: The HTTP response object returned by the server.

    Raises:
        HTTPError: If the HTTP request results in an error status code.
        JSONDecodeError: If the server response contains invalid JSON.
        Timeout: If the request exceeds the specified timeout.
        RequestException: For other request-related issues.
        Exception: For any unexpected errors during the request process.
    """

    session = requests.Session()
    LOG.info(f"Making a request to {url}")

    status_event = threading.Event()
    spinner_thread = threading.Thread(
        target=spinner,
        args=(
            status_event,
            "Waiting for response for server for " f"the request: {url}",
        ),
    )
    if show_spinner:
        status_event.set()
        spinner_thread.start()
    response = None
    try:
        response = session.request(
            method=method,
            url=url,
            headers=headers,
            data=data,
            json=json,
            stream=stream,
            timeout=timeout,
        )
        try:
            response.raise_for_status()
        # This is to make sure that the user gets to see the actual error
        # message which raise_for_status does not show
        except HTTPError:
            error_details = response.text
            if "application/json" in response.headers.get("Content-Type", "").lower():
                error_details = response.json()
            raise HTTPError(f"HTTP error {response.status_code}: {error_details}")

    except JSONDecodeError as e:
        raise JSONDecodeError(f"Invalid JSON: {e}", response.text, 0)
    except HTTPError as eh:
        raise HTTPError(f"HTTP error occurred: {eh}")
    except Timeout as et:
        raise Timeout(f"Timeout error occurred: {et}")
    except RequestException as e:
        raise RequestException(f"Request error occurred: {e}")
    except Exception as e:
        raise Exception(f"Unknown error occurred: {e}")
    finally:
        if show_spinner:
            status_event.clear()
            spinner_thread.join()
    return response


def build_api_url(
    url: str,
    api_endpoint: str,
    metadata_fields: Optional[list] = None,
    datasets_request: bool = True,
) -> str:
    """Builds a complete API URL by appending the endpoint and query parameters.

    This function constructs a URL by combining the base URL, API endpoint, and
    optional query parameters based on the provided metadata fields and whether
    the request targets datasets metadata or not.

    Args:
        url: The base URL of the API.
        api_endpoint: The specific endpoint to be appended to the base URL.
        metadata_fields: Optional list of metadata fields to include as query
        parameters.
        datasets_request: Indicates whether the request targets datasets.
        Defaults to True.

    Returns:
        A complete API URL string.
    """
    params = {}
    if datasets_request:
        params = _PORTAL_TYPE
        params[_FULL_SCHEMA] = "1"
    if metadata_fields:
        params[_METADATA_FIELDS] = ",".join(metadata_fields)
    if params:
        query_params = urlencode(params)
        return f"{url}/{api_endpoint}/?{query_params}"
    return f"{url}/{api_endpoint}"


def get_response_of_type(api_response: Response, data_type: Union[ResponseType, str]):
    """Extracts and validates the response content based on the specified data
    type.

    This function retrieves the content from an API response object, ensuring
    it matches the expected data type. Supported data types include JSON, text,
    and bytes.

    Args:
        api_response: The API response object to process.
        data_type: The expected type of the response content. Must be one of
            "json", "text", or "bytes".

    Returns:
        The response content in the specified data type.

    Raises:
        TypeError: If the provided `api_response` is not a `Response` object.
        ValueError: If `data_type` is not one of the supported types, or if the
            actual response content type does not match the expected `data_type`.
    """
    if not isinstance(api_response, Response):
        raise TypeError(
            f"Invalid input: response_data must be a Response, got "
            f"'{type(api_response).__name__}'."
        )

    valid_data_types = {"json", "text", "bytes"}
    if data_type not in valid_data_types:
        raise ValueError(
            f"Invalid data_type: {data_type}. Must be one of {valid_data_types}."
        )
    content_type = api_response.headers.get("Content-Type", "").lower()

    if "application/json" in content_type:
        response_data_type = "json"
        response = api_response.json()
    elif "text/html" in content_type:
        response_data_type = "text"
        response = api_response.text
    else:
        response_data_type = "bytes"
        response = api_response.content

    if response_data_type != data_type:
        raise ValueError(
            f"Type mismatch: Expected {data_type}, but response "
            f"is of type '{response_data_type}'."
        )

    return response


def spinner(status_event: threading.Event, message: str):
    """Displays a spinner with elapsed time for a single task until the event
    is set.

    This function prints a spinning animation and elapsed time message to the
    console/jupyter notebook output to indicate progress for a task. It stops
    when the provided event is cleared.

    Args:
        status_event: A threading event used to control the spinner's activity.
        message: A message to display alongside the spinner.
    """
    spinner_cycle = cycle(["◐", "◓", "◑", "◒"])
    start_time = time.time()

    while status_event.is_set():
        elapsed = int(time.time() - start_time)
        print(
            f"\rTask: {message}: {next(spinner_cycle)} Elapsed time:" f" {elapsed}s",
            end="",
            flush=True,
        )
        time.sleep(0.3)

    print(f"\rTask: {message}: Done!{' ' * 50}")
