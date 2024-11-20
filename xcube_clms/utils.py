from typing import Any, Optional
from urllib.parse import urlencode

import requests
from requests import JSONDecodeError, HTTPError, Timeout, RequestException
from xcube.core.store import DataTypeLike, DataStoreError, DATASET_TYPE

from xcube_clms.constants import (
    ACCEPT_HEADER,
    LOG,
    PORTAL_TYPE,
    FULL_SCHEMA,
    METADATA_FIELDS,
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


def make_api_request(
    url: str,
    headers: dict = ACCEPT_HEADER,
    data: dict = None,
    json: dict = None,
    method: str = "GET",
    stream: bool = False,
) -> dict:
    session = requests.Session()
    LOG.info(f"Making a request to {url}")
    try:
        response = session.request(
            method=method,
            url=url,
            headers=headers,
            data=data,
            json=json,
            stream=stream,
        )
        try:
            response.raise_for_status()
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
        content_type = response.headers.get("Content-Type", "").lower()
        if "application/json" in content_type:
            try:
                return {"type": "json", "response": response.json()}
            except JSONDecodeError as e:
                raise JSONDecodeError("Invalid JSON in response", e)
        elif "text/html" in content_type:
            return {"type": "text", "response": response}
        else:
            return {"type": "bytes", "response": response}

    except requests.exceptions.HTTPError as eh:
        raise HTTPError(f"HTTP error occurred. {eh}")
    except requests.exceptions.Timeout as et:
        raise Timeout(f"Timeout error occurred: {et}")
    except requests.exceptions.RequestException as e:
        raise RequestException(f"Request error occurred: {e}")


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


def get_response_of_type(response_data: dict, data_type: str):
    try:
        if data_type == "json":
            return response_data.get("response", {})
        if data_type == "text":
            return response_data.get("response", "")
        if data_type == "bytes":
            return response_data.get("response", b"")
    except TypeError:
        raise TypeError(f"Expected {data_type}. Got {response_data.get("type", "")}")


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


def convert_list_dict_to_list(data: list[dict[str, Any]], key: str) -> list[str]:
    return [d[key] for d in data if key in d]
