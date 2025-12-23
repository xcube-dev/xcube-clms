# The MIT License (MIT)
# Copyright (c) 2025 by the xcube development team and contributors
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

import re
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Any, Final, Literal, Optional, TypeAlias, Union
from urllib.parse import urlencode

import fsspec
import requests
import xarray as xr
from requests import HTTPError, JSONDecodeError, RequestException, Response, Timeout
from shapely import wkt
from xcube.core.store import (
    DATASET_TYPE,
    DataStoreError,
    DataTypeLike,
    PreloadedDataStore,
)

from .constants import (
    ACCEPT_HEADER,
    CLMS_API_URL,
    CLMS_DATA_ID_KEY,
    DATA_ID_SEPARATOR,
    DATASET_DOWNLOAD_INFORMATION,
    DOWNLOAD_FOLDER,
    DOWNLOADABLE_FILES_KEY,
    FULL_SOURCE,
    ITEMS_KEY,
    LOG,
    SEARCH_ENDPOINT,
    SUPPORTED_DATASET_SOURCES,
)

_RESULTS = "Results/"
_GEO_FILE_EXTS = (".tif", ".tiff")
_PORTAL_TYPE = {"portal_type": "DataSet"}
_ORIGINAL_FILENAME_KEY = "orig_filename"
_NAME_KEY = "name"
_FILENAME_KEY = "filename"
_BATCH = "batching"
_NEXT = "next"
_PREFERRED_CHUNK_SIZE: Final = 2000


ResponseType = Literal["json", "text", "bytes"]
DateLike: TypeAlias = datetime | date | str


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
    headers: Optional[dict[str, str]] = ACCEPT_HEADER,
    data: Optional[dict[str, Any]] = None,
    json: Optional[dict[str, Any]] = None,
    method: str = "GET",
    stream: bool = False,
    timeout: int = 100,
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
    LOG.debug(f"Making a request to {url}")
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
        response.raise_for_status()

    except HTTPError as e:
        # This is to make sure that the user gets to see the actual error
        # message which raise_for_status does not show
        error_details = response.text
        if "application/json" in response.headers.get("Content-Type", "").lower():
            try:
                error_details = response.json()
            except JSONDecodeError as json_e:
                LOG.error(f"Failed to decode JSON error response: {json_e}")
        new_error_message = (
            f"HTTP error {response.status_code}: {error_details}. Original error: {e}"
        )
        LOG.error(new_error_message)
        raise HTTPError(new_error_message, response=e.response) from e

    except (
        Timeout,
        RequestException,
    ) as e:
        LOG.error(f"An error occurred during the request to {url}: {e}")
        raise

    except Exception as e:
        LOG.error(f"Unknown error occurred: {e}")
        raise

    return response


def build_api_url(
    url: str,
    api_endpoint: str,
    extra_params: dict[str, str] | None = None,
    datasets_request: bool = False,
) -> str:
    """Builds a complete API URL by appending the endpoint and query parameters.

    This function constructs a URL by combining the base URL, API endpoint, and
    optional query parameters based on the provided metadata fields and whether
    the request targets datasets metadata or not.

    Args:
        url: The base URL of the API.
        api_endpoint: The specific endpoint to be appended to the base URL.
        extra_params: Optional dictionary of additional query parameters to
            include.
        datasets_request: Indicates whether the request targets datasets.
            Defaults to False.

    Returns:
        A complete API URL string.
    """
    params = {}
    _FULL_SCHEMA = "fullobjects"
    if datasets_request:
        params = _PORTAL_TYPE
        params[_FULL_SCHEMA] = "1"
    if extra_params:
        params.update(extra_params)
    if params:
        query_params = urlencode(params, doseq=True)
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
    elif "text" in content_type:
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


def get_spatial_dims(ds: xr.Dataset) -> (str, str):
    """Identifies the spatial coordinate names in a dataset.
    The function checks for common spatial dimension naming conventions: ("lat", "lon")
    or ("y", "x"). If neither pair is found, it raises a DataStoreError.

    Args:
        ds: The dataset to inspect.

    Returns:
        A tuple of strings representing the names of the spatial dimensions.

    Raises:
        DataStoreError: If no recognizable spatial dimensions are found.
    """
    if "lat" in ds and "lon" in ds:
        y_coord, x_coord = "lat", "lon"
    elif "y" in ds and "x" in ds:
        y_coord, x_coord = "y", "x"
    else:
        raise DataStoreError("No spatial dimensions found in dataset.")
    return y_coord, x_coord


def fetch_all_datasets() -> list[dict[str, Any]]:
    """Fetches all datasets from the CLMS API and their metadata.

    Returns:
        A list of dictionaries representing all datasets.
    """
    LOG.info(f"Fetching datasets metadata from {CLMS_API_URL}")
    datasets_info = []
    url = build_api_url(CLMS_API_URL, SEARCH_ENDPOINT, datasets_request=True)
    response_data = make_api_request(url)
    while True:
        response = get_response_of_type(response_data, "json")
        datasets_info.extend(response.get(ITEMS_KEY, []))
        next_page = response.get(_BATCH, {}).get(_NEXT)
        if not next_page:
            break
        response_data = make_api_request(next_page)
    LOG.info("Fetching complete.")
    return datasets_info


def get_extracted_component(
    datasets_info: list[dict[str, Any]],
    data_id,
    item_type: Literal["item", "product"] = "product",
) -> dict[str, Any] | None:
    """Extracts either an item or product from the list of datasets
    available.

    Product: The whole schema of the data product offering.
    Item: Schema to download one specific file.

    Args:
        datasets_info: Complete list of metadata of all datasets from CLMS API.
        data_id: The unique identifier for the dataset.
        item_type: 'item' to return downloadable item(s), 'product' to
                return the dataset entry.

    Returns:
        A dictionary representing the dataset item.

    Raises:
        ValueError: If the dataset item is not found or multiple items match.
    """

    def extract_dataset_component() -> list[dict[str, Any]] | list[Any]:
        if item_type == "item":
            if DATA_ID_SEPARATOR in data_id:
                clms_data_product_id, dataset_filename = data_id.split(
                    DATA_ID_SEPARATOR
                )

                matching_items = []

                for product in datasets_info:
                    if product[CLMS_DATA_ID_KEY] != clms_data_product_id:
                        continue

                    downloadable_files = product.get(DOWNLOADABLE_FILES_KEY, {}).get(
                        ITEMS_KEY, []
                    )

                    for item in downloadable_files:
                        if item.get("file") == dataset_filename:
                            matching_items.append(item)
                return matching_items

            for product in datasets_info:
                if product[CLMS_DATA_ID_KEY] == data_id:
                    dataset_download_info = product[DATASET_DOWNLOAD_INFORMATION][
                        ITEMS_KEY
                    ][0]
                    if (
                        dataset_download_info.get(FULL_SOURCE).lower()
                        in SUPPORTED_DATASET_SOURCES
                    ):
                        return [dataset_download_info]

            return []

        elif item_type == "product":
            return [
                product
                for product in datasets_info
                if data_id.split(DATA_ID_SEPARATOR)[0] == product[CLMS_DATA_ID_KEY]
            ]

        else:
            raise ValueError(
                f"Invalid item_type: {item_type}. Must be 'item' or 'product'."
            )

    dataset = extract_dataset_component()
    if len(dataset) > 1:
        LOG.warning(
            f"Expected one dataset for data_id: {data_id}, found {len(dataset)}."
        )
    elif len(dataset) < 1:
        LOG.warning(f"No dataset found for data_id: {data_id}")
        return None
    return dataset[0]


def get_dataset_download_info(dataset_id: str, file_id: str) -> dict:
    """Generates download information for a specific dataset ID and file ID.

    This function creates a dictionary containing dataset and file IDs,
    formatted as required by the CLMS API.

    Args:
        dataset_id: The identifier for the dataset product.
        file_id: The identifier for the file within the dataset product.

    Returns:
        A dictionary containing the dataset and file IDs.
    """
    return {
        "Datasets": [
            {
                "DatasetID": dataset_id,
                "FileID": file_id,
            }
        ]
    }


def get_authorization_header(access_token: str) -> dict:
    """Creates an authorization header using the provided access token.

    This function generates the HTTP authorization header required by the CLMS
    API requests, formatted with the Bearer token.

    Args:
        access_token: The access token to include in the header.

    Returns:
        A dictionary containing the authorization header.
    """
    return {"Authorization": f"Bearer {access_token}"}


def download_zip_data(
    cache_store: PreloadedDataStore, download_url: str, data_id: str
) -> None:
    """Downloads, extracts, and saves the dataset from the provided URL.

    Args:
        download_url: URL for downloading the dataset.
        data_id: Unique identifier of the dataset.
    """
    LOG.debug(f"Downloading zip file from {download_url}")

    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://source-website.com"}
    response = make_api_request(download_url, timeout=600, stream=True, headers=headers)
    chunk_size = 1024 * 1024  # 1 MB chunks

    with tempfile.NamedTemporaryFile(mode="wb", delete=True) as temp_file:
        temp_file_path = temp_file.name
        LOG.debug(f"Temporary file created at {temp_file_path}")

        for chunk in response.iter_content(chunk_size=chunk_size):
            temp_file.write(chunk)
            del chunk

        outer_zip_fs = fsspec.filesystem("zip", fo=temp_file_path)
        zip_contents = outer_zip_fs.ls(_RESULTS)
        actual_zip_file = None
        if len(zip_contents) == 1:
            if ".zip" in zip_contents[0][_FILENAME_KEY]:
                actual_zip_file = zip_contents[0]
        elif len(zip_contents) > 1:
            LOG.warn("Cannot handle more than one zip files at the moment.")
        else:
            LOG.warn("No downloadable zip file found inside.")
        if actual_zip_file:
            LOG.debug(
                f"Found one zip file {actual_zip_file.get(_ORIGINAL_FILENAME_KEY)}."
            )
            with outer_zip_fs.open(actual_zip_file[_NAME_KEY], "rb") as f:
                inner_zip_fs = fsspec.filesystem("zip", fo=f)

                geo_files = find_geo_in_dir(
                    "/",
                    inner_zip_fs,
                )
                if geo_files:
                    target_folder = cache_store.fs.sep.join(
                        [cache_store.root, DOWNLOAD_FOLDER, data_id]
                    )
                    cache_store.fs.makedirs(
                        target_folder,
                        exist_ok=True,
                    )
                    for geo_file in geo_files:
                        try:
                            with inner_zip_fs.open(geo_file, "rb") as source_file:
                                geo_file_name = geo_file.split("/")[-1]
                                geo_file_path = cache_store.fs.sep.join(
                                    [target_folder, geo_file_name]
                                )
                                with open(
                                    geo_file_path,
                                    "wb",
                                ) as dest_file:
                                    for chunk in iter(
                                        lambda: source_file.read(chunk_size),
                                        b"",
                                    ):
                                        dest_file.write(chunk)
                            LOG.debug(
                                f"The file {geo_file_name} has been successfully "
                                f"downloaded to {geo_file_path}"
                            )

                        except OSError as e:
                            LOG.error(f"Error occurred while reading/writing data. {e}")
                            raise
                        except Exception as e:
                            LOG.error(f"An unexpected error occurred: {e}")
                            raise

                else:
                    raise FileNotFoundError(
                        f"No {_GEO_FILE_EXTS} file found in the downloaded zip file to "
                        f"load"
                    )


def find_geo_in_dir(path: str, zip_fs: Any) -> list[str]:
    """Searches recursively a directory within a zip filesystem for geo
    files.

    Args:
        path: Path within the zip filesystem to start searching.
        zip_fs: Zip filesystem object supporting directory listing and file
         checks.

    Returns:
        list[str]: A list of geo file paths found within the specified
            directory.

    Notes:
        - A geo file is identified by its extension, which matches entries
            in `_GEO_FILE_EXTS`.
    """
    geo_file: list[str] = []
    contents = zip_fs.ls(path)
    for item in contents:
        if zip_fs.isdir(item[_NAME_KEY]):
            geo_file.extend(
                find_geo_in_dir(
                    item[_NAME_KEY],
                    zip_fs,
                )
            )
        else:
            if item[_NAME_KEY].endswith(_GEO_FILE_EXTS):
                LOG.debug(f"Found geo file: {item[_NAME_KEY]}")
                filename = item[_NAME_KEY]
                geo_file.append(filename)
    return geo_file


def find_easting_northing(name: str) -> Optional[str]:
    """Finds the easting/northing coordinate pattern in the provided filename.

    This function searches for a specific pattern, "E##N##", in a string
    and returns the first match if found.

    Args:
        name: The string to search for the easting/northing pattern.

    Returns:
        The matched coordinate string if found, otherwise None.
    """
    match = re.search(r"[E]\d{2}[N]\d{2}", name)
    if match:
        return match.group(0)
    return None


def cleanup_dir(folder_path: Path | str, fs=None, keep_extension=None):
    """Removes all files from a directory, retaining only those with the
    specified extension in the root directory.

    Args:
        folder_path: The path to the directory to clean up.
        fs: A fsspec filesystem object. If None, the local filesystem is used.
            Optional.
        keep_extension: The file extension to retain. Optional
    """
    folder_path = str(folder_path)
    fs = fs or fsspec.filesystem("file")

    if not fs.isdir(folder_path):
        raise ValueError(f"The specified path {folder_path} is not a directory.")

    for item in fs.listdir(folder_path):
        item_path = item["name"]
        try:
            # Adding the `not item_path.endswith(keep_extension)` condition
            # here as `.zarr` files are recognized as folders
            if fs.isdir(item_path) and (
                keep_extension is None
                or (keep_extension and not item_path.endswith(keep_extension))
            ):
                fs.rm(item_path, recursive=True)
                LOG.debug(f"Deleted directory: {item_path}")
            else:
                if keep_extension and item_path.endswith(keep_extension):
                    LOG.debug(f"Kept file: {item_path}")
                else:
                    fs.rm(item_path)
                    LOG.debug(f"Deleted file: {item_path}")
        except Exception as e:
            LOG.error(f"Failed to delete {item_path}: {e}")
    LOG.debug("Cleaning up finished")


def get_tile_size(tile_size: tuple[int, int] | int | None):
    if tile_size is None:
        tile_size = (_PREFERRED_CHUNK_SIZE, _PREFERRED_CHUNK_SIZE)
    elif isinstance(tile_size, int):
        tile_size = (tile_size, tile_size)
    else:
        tile_size = tile_size

    return tile_size


def detect_format(url: str):
    if url.endswith(".nc"):
        return "netcdf"
    elif url.endswith((".tif", ".tiff")):
        return "geotiff"
    else:
        return "unknown"


def normalize_time_range(time_range: tuple[DateLike, DateLike]):
    """
    Normalize date/datetime inputs (objects or strings) to 'YYYY-MM-DD'.
    Preserves None values.
    """
    normalized = []

    for value in time_range:
        if value is None:
            normalized.append(None)
            continue

        if isinstance(value, datetime):
            normalized.append(value.date().isoformat())
            continue

        if isinstance(value, date):
            normalized.append(value.isoformat())
            continue

        if isinstance(value, str):
            v = value

            if v.endswith("Z"):
                v = v[:-1]

            try:
                dt = datetime.fromisoformat(v)
                normalized.append(dt.date().isoformat())
                continue
            except ValueError:
                raise ValueError(f"Invalid date string: {value}")

        raise TypeError(f"Expected date, datetime, str, or None; got {type(value)}")

    return normalized


def to_bbox(
    geometry: tuple[float, float, float, float] | str,
) -> tuple[float, float, float, float]:
    if isinstance(geometry, tuple) and is_valid_bbox(geometry):
        return geometry

    if isinstance(geometry, str):
        poly = wkt.loads(geometry)
        return poly.bounds

    raise ValueError(f"Unsupported geometry format: {geometry}, {type(geometry)}")


def is_valid_bbox(bbox: tuple[float, float, float, float]) -> bool:
    minx, miny, maxx, maxy = bbox
    return (
        -180 <= minx <= 180
        and -180 <= maxx <= 180
        and -90 <= miny <= 90
        and -90 <= maxy <= 90
    )
