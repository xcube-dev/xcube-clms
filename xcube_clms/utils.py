import os
import re
import threading
import time
from datetime import datetime, timedelta
from itertools import cycle
from typing import Optional, Any, Union, Literal
from urllib.parse import urlencode

import requests
from requests import JSONDecodeError, HTTPError, Timeout, RequestException, \
    Response
from tqdm.notebook import tqdm
from xcube.core.store import DataTypeLike, DataStoreError, DATASET_TYPE

from xcube_clms.constants import (
    ACCEPT_HEADER,
    LOG,
    PORTAL_TYPE,
    FULL_SCHEMA,
    METADATA_FIELDS,
    TIME_TO_EXPIRE,
    KEEP_EXTENSION,
)


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
            if "application/json" in response.headers.get("Content-Type", "").lower():
                try:
                    error_details = response.json()
                    raise HTTPError(
                        f"HTTP error {response.status_code}: {error_details}"
                    )
                except JSONDecodeError as e:
                    raise JSONDecodeError(f"Unable to parse JSON. {e}")
            raise HTTPError(f"HTTP error {response.status_code}: {response.text}")

    except JSONDecodeError as e:
        raise JSONDecodeError("Invalid JSON in response", e)
    except requests.exceptions.HTTPError as eh:
        raise HTTPError(f"HTTP error occurred. {eh}")
    except requests.exceptions.Timeout as et:
        raise Timeout(f"Timeout error occurred: {et}")
    except requests.exceptions.RequestException as e:
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
    params = {}
    if datasets_request:
        params = PORTAL_TYPE
        params[FULL_SCHEMA] = "1"
    if metadata_fields:
        params[METADATA_FIELDS] = ",".join(metadata_fields)
    if params:
        query_params = urlencode(params)
        return f"{url}/{api_endpoint}/?{query_params}"
    return f"{url}/{api_endpoint}"


def get_response_of_type(api_response: Response, data_type: Union[ResponseType, str]):

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


def get_dataset_download_info(dataset_id: str, file_id: str) -> dict:
    return {
        "Datasets": [
            {
                "DatasetID": dataset_id,
                "FileID": file_id,
            }
        ]
    }


def get_authorization_header(access_token: str) -> dict:
    return {"Authorization": f"Bearer {access_token}"}


def has_expired(download_available_time):
    given_time = datetime.fromisoformat(download_available_time)
    current_time = datetime.now()
    if (current_time - given_time) > timedelta(hours=TIME_TO_EXPIRE):
        return True
    else:
        return False


def spinner(status_event, message):
    """
    Displays a spinner with elapsed time for a single task until the event is set.
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


def find_easting_northing(name: str):
    match = re.search(r"[E]\d{2}[N]\d{2}", name)
    if match:
        return match.group(0)
    return None


def cleanup_dir(folder_path, keep_extension=None):
    if keep_extension is None:
        keep_extension = KEEP_EXTENSION

    for filename in tqdm(
        os.listdir(folder_path), desc=f"Cleaning up directory {folder_path}"
    ):
        file_path = os.path.join(folder_path, filename)

        if os.path.isfile(file_path) and not filename.endswith(keep_extension):
            os.remove(file_path)
            LOG.debug(f"Deleted: {file_path}")
        else:
            LOG.debug(f"Kept: {file_path}")
    LOG.info(f"Cleaning up finished")
