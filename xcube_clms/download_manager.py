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

import os
import tempfile
from datetime import datetime, timedelta
from typing import Any

import fsspec
from tqdm.notebook import tqdm

from xcube_clms.api_token_handler import ClmsApiTokenHandler
from xcube_clms.constants import (
    LOG,
    CANCELLED,
    PENDING,
    COMPLETE,
    TASK_STATUS_ENDPOINT,
    ACCEPT_HEADER,
    CONTENT_TYPE_HEADER,
    DOWNLOAD_ENDPOINT,
    TIME_TO_EXPIRE,
)
from xcube_clms.utils import (
    make_api_request,
    get_response_of_type,
    build_api_url,
)

_UID_KEY = "UID"
_DATASET_DOWNLOAD_INFORMATION_KEY = "dataset_download_information"
_ID_KEY = "@id"
_FILE_ID_KEY = "FileID"
_DOWNLOAD_URL_KEY = "DownloadURL"
_STATUS_KEY = "Status"
_DATASETS_KEY = "Datasets"
_DATASET_ID_KEY = "DatasetID"
_FILENAME_KEY = "filename"
_NAME_KEY = "name"
_TITLE_KEY = "title"
_PATH_KEY = "path"
_SOURCE_KEY = "source"
_FULL_SOURCE_KEY = "full_source"
_TASK_IDS_KEY = "TaskIds"
_TASK_ID_KEY = "TaskID"
_DOWNLOAD_AVAILABLE_TIME_KEY = "FinalizationDateTime"
_ORIGINAL_FILENAME_KEY = "orig_filename"
_STATUS_PENDING = ["Queued", "In_progress"]
_STATUS_COMPLETE = ["Finished_ok"]
_STATUS_CANCELLED = ["Cancelled"]
_UNDEFINED = "UNDEFINED"
_RESULTS = "Results/"
_NOT_SUPPORTED_LIST = ["WEKEO", "LEGACY", "LANDCOVER"]
_GEO_FILE_EXTS = (".tif", ".tiff", ".nc")
_ITEMS_KEY = "items"


class DownloadTaskManager:
    """Manages tasks for downloading datasets from the CLMS API."""

    def __init__(
        self,
        token_handler: ClmsApiTokenHandler,
        url: str,
        path: str,
        disable_tqdm_progress: bool | None = None,
    ) -> None:
        """Initializes the DownloadTaskManager

        Args:
            token_handler: TokenHandler instance for API authentication.
            url: Base URL for the API.
            path: Path where downloaded data will be stored.
        """
        self._token_handler = token_handler
        self._api_token = self._token_handler.api_token
        self._url = url
        self.path = path
        self.disable_tqdm_progress = disable_tqdm_progress

    def request_download(self, data_id: str, item: dict, product: dict) -> str:
        """Submits a download request for a specific dataset.

        If a request does not exist, it sends a new one. If it does, it returns
        the existing task ID.

        Args:
            data_id: Unique identifier of the dataset.
            item: Metadata for the specific file to download.
            product: Metadata for the dataset containing the file.

        Returns:
            str: Task ID of the submitted download request.

        Raises:
            AssertionError: If the dataset is unsupported.
        """
        self._token_handler.refresh_token()

        # This is to make sure that there are pre-packaged files available for
        # download. Without this, the API throws the following error: Error,
        # the FileID is not valid.
        # We check for path and source based on the API code here:
        # https://github.com/eea/clms.downloadtool/blob/master/clms/downloadtool/api/services/datarequest_post/post.py#L177-L196

        path = item.get(_PATH_KEY, "")
        source = item.get(_SOURCE_KEY, "")

        if (path == "") and (source == ""):
            LOG.warning(f"No prepackaged downloadable items available for {data_id}")

        # This is to make sure that we do not send requests for currently for
        # unsupported datasets.

        full_source = (
            product.get(_DATASET_DOWNLOAD_INFORMATION_KEY)
            .get(_ITEMS_KEY)[0]
            .get(_FULL_SOURCE_KEY, "")
        )

        assert full_source not in _NOT_SUPPORTED_LIST, (
            f"This data product: {product[_TITLE_KEY]} is not yet supported in "
            f"this plugin yet."
        )

        status, task_id = self.get_current_requests_status(
            dataset_id=product[_UID_KEY], file_id=item[_ID_KEY]
        )

        if status == COMPLETE or status == PENDING:
            LOG.debug(
                f"Download request with task id {task_id} "
                f"{'already completed' if status == 'COMPLETE' 
                else 'is in queue'} for data id: {data_id}"
            )
            return task_id
        if status == _UNDEFINED:
            LOG.debug(
                f"Download request does not exists or has expired for "
                f"data id:"
                f" {data_id}"
            )

        if status == CANCELLED:
            LOG.debug(
                f"Download request was cancelled for "
                f"data id:"
                f" {data_id}. Re-requesting now."
            )

        download_request_url, headers, json = self._prepare_download_request(
            data_id, item, product
        )

        response_data = make_api_request(
            method="POST", url=download_request_url, headers=headers, json=json
        )
        response = get_response_of_type(response_data, "json")
        task_ids = response.get(_TASK_IDS_KEY)
        assert (
            len(task_ids) == 1
        ), f"Expected API response with 1 task_id, got {len(task_ids)}"
        task_id = task_ids[0].get(_TASK_ID_KEY)
        LOG.debug(f"Download Requested with Task ID : {task_id}")
        return task_id

    def get_download_url(self, task_id: str) -> tuple[str, int]:
        """Retrieves the download URL and file size for a completed download
        task.

        Args:
            task_id: Task ID for which to retrieve the download URL.

        Returns:
            tuple[str, int]: A tuple containing the download URL and the file
             size in bytes.

        Raises:
            Exception: If the task has not completed or no download URL is
            available.
        """
        self._token_handler.refresh_token()

        headers = ACCEPT_HEADER.copy()
        headers.update(get_authorization_header(self._api_token))

        url = build_api_url(self._url, TASK_STATUS_ENDPOINT, datasets_request=False)
        response_data = make_api_request(url=url, headers=headers)
        response = get_response_of_type(response_data, "json")

        for key in response:
            if key == task_id:
                status = response[key][_STATUS_KEY]
                if status in _STATUS_COMPLETE:
                    return (
                        response[key][_DOWNLOAD_URL_KEY],
                        response[key]["FileSize"],
                    )
                else:
                    raise Exception(
                        f"Task ID {task_id} has not yet finished. "
                        "No download url available yet."
                    )

    def _prepare_download_request(
        self, data_id: str, item: dict, product: dict
    ) -> tuple[str, dict, dict]:
        """Prepares the API request details for downloading a dataset.

        Args:
            data_id: Unique identifier of the dataset.
            item: Metadata for the specific file to download.
            product: Metadata for the dataset containing the file.

        Returns:
            tuple[str, dict, dict]: A tuple containing the request URL,
                headers, and JSON payload.
        """
        LOG.debug(f"Preparing download request for {data_id}")

        dataset_id = product[_UID_KEY]
        file_id = item[_ID_KEY]
        json = get_dataset_download_info(
            dataset_id=dataset_id,
            file_id=file_id,
        )
        url = build_api_url(self._url, DOWNLOAD_ENDPOINT, datasets_request=False)
        if not self._api_token:
            self._token_handler.refresh_token()
        headers = ACCEPT_HEADER.copy()
        headers.update(CONTENT_TYPE_HEADER)
        headers.update(get_authorization_header(self._api_token))

        return url, headers, json

    def get_current_requests_status(
        self,
        dataset_id: str | None = None,
        file_id: str | None = None,
        task_id: str | None = None,
    ) -> tuple[str, str]:
        """Checks the status of existing download request task.

        The user can either provide the dataset_id and file_id or just the
        task_id to enquire the status of the request.

        The sorting is performed based on the priority and timestamps so that
        we have the result of the latest requests in the decreasing order of
        priorities.

        Args:
            dataset_id: Dataset ID to filter tasks (optional).
            file_id: File ID to filter tasks (optional).
            task_id: Task ID to filter tasks (optional).

        Returns:
            tuple[str, str]: A tuple containing the status
                (e.g., COMPLETE, PENDING) and task ID.

        Notes:
        """
        self._token_handler.refresh_token()
        headers = ACCEPT_HEADER.copy()
        headers.update(get_authorization_header(self._api_token))

        url = build_api_url(self._url, TASK_STATUS_ENDPOINT, datasets_request=False)
        response_data = make_api_request(url=url, headers=headers)
        response = get_response_of_type(response_data, "json")

        status_priority = {
            "Finished_ok": 1,  # Complete
            "Queued": 2,  # Pending
            "In_progress": 2,  # Pending
            "Cancelled": 3,  # Cancelled
        }

        latest_entries = {status: {} for status in status_priority.keys()}

        for key in response:
            status = response[key][_STATUS_KEY]
            datasets = response[key][_DATASETS_KEY]
            requested_data_id = datasets[0][_DATASET_ID_KEY]
            requested_file_id = datasets[0][_FILE_ID_KEY]
            timestamp = (
                response[key][_DOWNLOAD_AVAILABLE_TIME_KEY]
                if status in {"Finished_ok", "Cancelled"}
                else None
            )  # Only get timestamp for Completed or Cancelled
            condition = (
                ((dataset_id == requested_data_id) and (file_id == requested_file_id))
                if dataset_id
                else (key == task_id)
            )
            if condition:
                current_entry = {
                    "status": status,
                    "key": key,
                    "timestamp": timestamp,
                    "response": response[key],
                }

                if status in latest_entries:
                    existing_entry = latest_entries[status]
                    if not existing_entry or (
                        timestamp and (timestamp > existing_entry.get("timestamp", ""))
                    ):
                        latest_entries[status] = current_entry

        for status in sorted(
            status_priority, key=lambda s: status_priority[s], reverse=False
        ):
            latest_entry = latest_entries[status]
            if latest_entry:
                key = latest_entry["key"]
                entry_response = latest_entry["response"]
                if status in _STATUS_COMPLETE:
                    if not has_expired(entry_response[_DOWNLOAD_AVAILABLE_TIME_KEY]):
                        return COMPLETE, key
                elif status in _STATUS_PENDING:
                    return PENDING, key
                elif status in _STATUS_CANCELLED:
                    return CANCELLED, key

        return _UNDEFINED, ""

    def download_data(self, download_url: str, data_id: str) -> None:
        """Downloads, extracts, and saves the dataset from the provided URL.

        Args:
            download_url: URL for downloading the dataset.
            data_id: Unique identifier of the dataset.
        """
        LOG.debug(f"Downloading zip file from {download_url}")

        response = make_api_request(download_url, timeout=600, stream=True)
        chunk_size = 1024 * 1024  # 1 MB chunks

        with tempfile.NamedTemporaryFile(mode="wb", delete=True) as temp_file:
            temp_file_path = temp_file.name
            LOG.debug(f"Temporary file created at {temp_file_path}")

            for chunk in response.iter_content(chunk_size=chunk_size):
                temp_file.write(chunk)
                del chunk

            fs = fsspec.filesystem("zip", fo=temp_file_path)
            zip_contents = fs.ls(_RESULTS)
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
                    f"Found one zip file "
                    f"{actual_zip_file.get(_ORIGINAL_FILENAME_KEY)}."
                )
                with fs.open(actual_zip_file[_NAME_KEY], "rb") as f:
                    zip_fs = fsspec.filesystem("zip", fo=f)

                    geo_files = DownloadTaskManager._find_geo_in_dir(
                        "/",
                        zip_fs,
                    )
                    if geo_files:
                        target_folder = os.path.join(self.path, data_id + "/").__str__()
                        os.makedirs(
                            os.path.dirname(target_folder),
                            exist_ok=True,
                        )
                        for geo_file in tqdm(
                            geo_files,
                            desc="Extracting geo files for task_id {task_id}",
                            disable=self.disable_tqdm_progress,
                        ):
                            try:
                                with zip_fs.open(geo_file, "rb") as source_file:
                                    geo_file_name = geo_file.split("/")[-1]
                                    geo_file_path = os.path.join(
                                        target_folder, geo_file_name
                                    )
                                    with open(
                                        geo_file_path,
                                        "wb",
                                    ) as dest_file:
                                        for chunk in tqdm(
                                            iter(
                                                lambda: source_file.read(chunk_size),
                                                b"",
                                            ),
                                            desc=f"Extracting geo file {geo_file_name}",
                                            disable=self.disable_tqdm_progress,
                                        ):
                                            dest_file.write(chunk)
                                LOG.debug(
                                    f"The file {geo_file_name} has been successfully "
                                    f"downloaded to {geo_file_path}"
                                )
                            except OSError as e:
                                LOG.error(f"Error occurred while writing data. {e}")
                                raise
                            except UnicodeDecodeError as e:
                                LOG.error(
                                    f"Decoding error: {e}. File might not be text "
                                    f"or encoding is incorrect."
                                )
                                raise
                            except Exception as e:
                                LOG.error(f"An unexpected error occurred: {e}")
                                raise

                    else:
                        raise FileNotFoundError(
                            "No file found in the downloaded zip file to load"
                        )

    @staticmethod
    def _find_geo_in_dir(path: str, zip_fs: Any) -> list[str]:
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
                in `GEO_FILE_EXTS`.
        """
        geo_file: list[str] = []
        contents = zip_fs.ls(path)
        for item in contents:
            if zip_fs.isdir(item[_NAME_KEY]):
                geo_file.extend(
                    DownloadTaskManager._find_geo_in_dir(
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


def has_expired(download_available_time: str) -> bool:
    """Checks if the download availability time has expired.

    This function compares the provided time against the current time to
    determine whether the download window has expired.

    Args:
        download_available_time: The string representing the timestamp when the
        download was made available.

    Returns:
        True if the download window has expired, otherwise False.
    """
    given_time = datetime.fromisoformat(download_available_time)
    current_time = datetime.now()
    if (current_time - given_time) > timedelta(hours=TIME_TO_EXPIRE):
        return True
    else:
        return False
