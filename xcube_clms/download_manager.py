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
from typing import Any

import fsspec
from tqdm.notebook import tqdm

from xcube_clms.api_token_handler import CLMSAPITokenHandler
from xcube_clms.constants import (
    NAME_KEY,
    GEO_FILE_EXTS,
    LOG,
    ORIGINAL_FILENAME_KEY,
    FILENAME_KEY,
    RESULTS,
    UNDEFINED,
    CANCELLED,
    STATUS_CANCELLED,
    STATUS_PENDING,
    PENDING,
    COMPLETE,
    DOWNLOAD_AVAILABLE_TIME_KEY,
    STATUS_COMPLETE,
    FILE_ID_KEY,
    DATASET_ID_KEY,
    DATASETS_KEY,
    STATUS_KEY,
    JSON_TYPE,
    TASK_STATUS_ENDPOINT,
    ACCEPT_HEADER,
    CONTENT_TYPE_HEADER,
    PATH_KEY,
    SOURCE_KEY,
    DATASET_DOWNLOAD_INFORMATION_KEY,
    ITEMS_KEY,
    FULL_SOURCE_KEY,
    NOT_SUPPORTED_LIST,
    TITLE_KEY,
    UID_KEY,
    ID_KEY,
    TASK_IDS_KEY,
    TASK_ID_KEY,
    DOWNLOAD_URL_KEY,
    DOWNLOAD_ENDPOINT,
)
from xcube_clms.utils import (
    make_api_request,
    has_expired,
    get_response_of_type,
    build_api_url,
    get_authorization_header,
    get_dataset_download_info,
)


class DownloadTaskManager:
    """
    Manages tasks for downloading datasets from the CLMS API, including task
    creation, download request status monitoring, and data extraction.
    """

    def __init__(self, token_handler: CLMSAPITokenHandler, url: str, path: str) -> None:
        """
        Initializes the DownloadTaskManager with API token handler, base URL,
         and file storage path.

        Args:
            token_handler: TokenHandler instance for API authentication.
            url: Base URL for the API.
            path: Path where downloaded data will be stored.
        """
        self._token_handler = token_handler
        self._api_token = self._token_handler.api_token
        self._url = url
        self.path = path

    def request_download(self, data_id: str, item: dict, product: dict) -> str:
        """
        Submits a download request for a specific dataset if it does not
        already exist. If it does, it returns the existing task ID.

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

        path = item.get(PATH_KEY, "")
        source = item.get(SOURCE_KEY, "")

        if (path == "") and (source == ""):
            LOG.warning(f"No prepackaged downloadable items available for {data_id}")

        # This is to make sure that we do not send requests for currently for
        # unsupported datasets.

        full_source = (
            product.get(DATASET_DOWNLOAD_INFORMATION_KEY)
            .get(ITEMS_KEY)[0]
            .get(FULL_SOURCE_KEY, "")
        )

        assert full_source not in NOT_SUPPORTED_LIST, (
            f"This data product: {product[TITLE_KEY]} is not yet supported in "
            f"this plugin yet."
        )

        status, task_id = self.get_current_requests_status(
            dataset_id=product[UID_KEY], file_id=item[ID_KEY]
        )

        if status == COMPLETE or status == PENDING:
            LOG.info(
                f"Download request with task id {task_id} "
                f"{'already completed' if status == 'COMPLETE' 
                else 'is in queue'} for data id: {data_id}"
            )
            return task_id
        if status == UNDEFINED:
            LOG.info(
                f"Download request does not exists or has expired for "
                f"data id:"
                f" {data_id}"
            )

        if status == CANCELLED:
            LOG.info(
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
        response = get_response_of_type(response_data, JSON_TYPE)
        task_ids = response.get(TASK_IDS_KEY)
        assert (
            len(task_ids) == 1
        ), f"Expected API response with 1 task_id, got {len(task_ids)}"
        task_id = task_ids[0].get(TASK_ID_KEY)
        LOG.info(f"Download Requested with Task ID : {task_id}")
        return task_id

    def get_download_url(self, task_id: str) -> tuple[str, int]:
        """
        Retrieves the download URL and file size for a completed download task.

        Args:
            task_id: Task ID for which to retrieve the download URL.

        Returns:
            tuple[str, int]: A tuple containing the download URL and the file size in bytes.

        Raises:
            Exception: If the task has not completed or no download URL is available.
        """
        self._token_handler.refresh_token()

        headers = ACCEPT_HEADER.copy()
        headers.update(get_authorization_header(self._api_token))

        url = build_api_url(self._url, TASK_STATUS_ENDPOINT, datasets_request=False)
        response_data = make_api_request(url=url, headers=headers)
        response = get_response_of_type(response_data, JSON_TYPE)

        for key in response:
            if key == task_id:
                status = response[key][STATUS_KEY]
                if status in STATUS_COMPLETE:
                    return (
                        response[key][DOWNLOAD_URL_KEY],
                        response[key]["FileSize"],
                    )
                else:
                    raise Exception(
                        f"Task ID {task_id} has not yet finished. No download url available yet."
                    )

    def _prepare_download_request(
        self, data_id: str, item: dict, product: dict
    ) -> tuple[str, dict, dict]:
        """
        Prepares the API request details for downloading a dataset.

        Args:
            data_id: Unique identifier of the dataset.
            item: Metadata for the specific file to download.
            product: Metadata for the dataset containing the file.

        Returns:
            tuple[str, dict, dict]: A tuple containing the request URL,
                headers, and JSON payload.
        """
        LOG.info(f"Preparing download request for {data_id}")

        dataset_id = product[UID_KEY]
        file_id = item[ID_KEY]
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
        """
        Checks the status of existing download request tasks.
        The user can either provide the dataset_id and file_id or just the
        task_id to enquire the status of the request.

        We perform sorting based on the priority and timestamps so that we have
        the result of the latest requests in the decreasing order of priorities.

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
        response = get_response_of_type(response_data, JSON_TYPE)

        status_priority = {
            "Finished_ok": 1,  # Complete
            "Queued": 2,  # Pending
            "In_progress": 2,  # Pending
            "Cancelled": 3,  # Cancelled
        }

        latest_entries = {status: {} for status in status_priority.keys()}

        for key in response:
            status = response[key][STATUS_KEY]
            datasets = response[key][DATASETS_KEY]
            requested_data_id = datasets[0][DATASET_ID_KEY]
            requested_file_id = datasets[0][FILE_ID_KEY]
            timestamp = (
                response[key][DOWNLOAD_AVAILABLE_TIME_KEY]
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
                if status in STATUS_COMPLETE:
                    if not has_expired(entry_response[DOWNLOAD_AVAILABLE_TIME_KEY]):
                        return COMPLETE, key
                elif status in STATUS_PENDING:
                    return PENDING, key
                elif status in STATUS_CANCELLED:
                    return CANCELLED, key

        return UNDEFINED, ""

    def download_data(
        self, download_url: str, file_size: int, task_id: str, data_id: str
    ) -> None:
        """
        Downloads a dataset from the provided URL, extracts files, and saves
        them.

        Args:
            download_url: URL for downloading the dataset.
            file_size: Size of the dataset in bytes.
            task_id: Task ID associated with the dataset.
            data_id: Unique identifier of the dataset.
        """
        LOG.info(f"Downloading zip file from {download_url}")

        response = make_api_request(download_url, timeout=600, stream=True)
        chunk_size = 1024 * 1024  # 1 MB chunks

        with tempfile.NamedTemporaryFile(mode="wb", delete=True) as temp_file:
            temp_file_path = temp_file.name
            LOG.info(f"Temporary file created at {temp_file_path}")

            progress_bar = tqdm(
                response.iter_content(chunk_size=chunk_size),
                desc=f"Downloading zip file for task: {task_id}",
                total=file_size // chunk_size,
                unit_scale=True,
            )

            for chunk in progress_bar:
                temp_file.write(chunk)
                progress_bar.update(len(chunk))
                del chunk
            progress_bar.close()

            fs = fsspec.filesystem("zip", fo=temp_file_path)
            zip_contents = fs.ls(RESULTS)
            actual_zip_file = None
            if len(zip_contents) == 1:
                if ".zip" in zip_contents[0][FILENAME_KEY]:
                    actual_zip_file = zip_contents[0]
            elif len(zip_contents) > 1:
                LOG.warn("Cannot handle more than one zip files at the moment.")
            else:
                LOG.warn("No downloadable zip file found inside.")
            if actual_zip_file:
                LOG.info(
                    f"Found one zip file "
                    f"{actual_zip_file.get(ORIGINAL_FILENAME_KEY)}."
                )
                with fs.open(actual_zip_file[NAME_KEY], "rb") as f:
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
                                        ):
                                            dest_file.write(chunk)
                                LOG.info(
                                    f"The file {geo_file_name} has been successfully "
                                    f"downloaded to {geo_file_path}"
                                )
                            except OSError as e:
                                raise OSError(f"Error occurred while writing data. {e}")
                            except UnicodeDecodeError as e:
                                raise ValueError(
                                    f"Decoding error: {e}. File might not be text "
                                    f"or encoding is incorrect."
                                )
                            except Exception as e:
                                raise Exception(f"An unexpected error occurred: {e}")

                    else:
                        raise FileNotFoundError(
                            "No file found in the downloaded zip file to load"
                        )

    @staticmethod
    def _find_geo_in_dir(path: str, zip_fs: Any) -> list[str]:
        """
        Recursively searches a directory within a zip filesystem for geo files.

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
            - Logs details of each discovered geo file.
        """
        geo_file: list[str] = []
        contents = zip_fs.ls(path)
        for item in contents:
            if zip_fs.isdir(item[NAME_KEY]):
                geo_file.extend(
                    DownloadTaskManager._find_geo_in_dir(
                        item[NAME_KEY],
                        zip_fs,
                    )
                )
            else:
                if item[NAME_KEY].endswith(GEO_FILE_EXTS):
                    LOG.info(f"Found geo file: {item[NAME_KEY]}")
                    filename = item[NAME_KEY]
                    geo_file.append(filename)
        return geo_file
