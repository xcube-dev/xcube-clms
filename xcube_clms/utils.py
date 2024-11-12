import requests
from xcube.core.store import DataTypeLike, DataStoreError, DATASET_TYPE

from xcube_clms.constants import ACCEPT_HEADER, LOG


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
    retries: int = 3,
    method: str = "GET",
) -> dict:
    session = requests.Session()
    attempt = 0

    while attempt <= retries:
        try:
            response = session.request(
                method=method,
                url=url,
                headers=headers,
                data=data,
                json=json,
            )

            response.raise_for_status()

            return response.json()

        except Exception as e:
            LOG.error(f"Failed to parse JSON response: {e}")

        attempt += 1
        if attempt <= retries:
            LOG.warning(f"Retrying request with attempt no. {attempt})...")

    LOG.error(f"All retries exhausted for URL: {url}")
    return {}


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
