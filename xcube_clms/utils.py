from urllib.error import HTTPError

import requests
from requests import RequestException
from xcube.core.store import DataTypeLike, DataStoreError, DATASET_TYPE

from build.lib.xcube_clms.constants import HEADERS, LOG


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


def make_api_request(url: str, headers: dict = HEADERS, data: dict = None) -> dict:
    try:
        if data:
            response = requests.get(url, headers=headers, data=data)
        else:
            response = requests.get(url, headers=headers)

        response.raise_for_status()
        return response.json()
    except (HTTPError, RequestException, ValueError) as err:
        LOG.error(f"API error: {err}")
        return {}
