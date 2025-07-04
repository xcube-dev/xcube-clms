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

import tempfile
import time
from datetime import datetime
from typing import Any

import fsspec
from requests import RequestException
from xcube.core.store import PreloadedDataStore

from xcube_clms.api_token_handler import ClmsApiTokenHandler
from xcube_clms.constants import (
    ACCEPT_HEADER,
    DOWNLOAD_FOLDER,
    GET_DOWNLOAD_FILE_URLS_ENDPOINT,
    SUPPORTED_NON_EEA_DATASET_SOURCES,
    ALL_DATASET_SOURCES,
)
from xcube_clms.constants import CANCELLED
from xcube_clms.constants import COMPLETE
from xcube_clms.constants import CONTENT_TYPE_HEADER
from xcube_clms.constants import DOWNLOAD_ENDPOINT
from xcube_clms.constants import LOG
from xcube_clms.constants import PENDING
from xcube_clms.constants import TASK_STATUS_ENDPOINT
from xcube_clms.utils import build_api_url
from xcube_clms.utils import get_response_of_type
from xcube_clms.utils import make_api_request

_UID_KEY = "UID"
_DATASET_DOWNLOAD_INFORMATION_KEY = "dataset_download_information"
_CHARACTERISTICS_TEMPORAL_EXTENT = "characteristics_temporal_extent"
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
_NOT_SUPPORTED_LIST = [
    x for x in ALL_DATASET_SOURCES if x not in SUPPORTED_NON_EEA_DATASET_SOURCES
]
_GEO_FILE_EXTS = (".tif", ".tiff")
_ITEMS_KEY = "items"
_MAX_RETRIES = 7


class DownloadTaskManager:
    """Manages tasks for downloading datasets from the CLMS API."""

    def __init__(
        self,
        token_handler: ClmsApiTokenHandler,
        url: str,
        cache_store: PreloadedDataStore,
    ) -> None:
        self._token_handler = token_handler
        self._api_token = self._token_handler.api_token
        self._url = url
        self.cache_store = cache_store
        self.fs = cache_store.fs
        self.download_folder = self.fs.sep.join(
            [self.cache_store.root, DOWNLOAD_FOLDER]
        )

    def request_download(
        self, data_id: str, item: dict, product: dict
    ) -> str | list[str]:
        """Submits a download request for a specific dataset.

        If a request does not exist, it sends a new one. If it does, it returns
        the existing task ID.

        Args:
            data_id: Unique identifier of the dataset.
            item: Metadata for the specific file to download.
            product: Metadata for the dataset containing the file.

        Returns:
            str: Task ID of the submitted download request. or
            list[str]: List of download URLs

        Raises:
            AssertionError: If the dataset is unsupported.
            AssertionError: If multiple task ids are created for the same
                request.
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
            LOG.info(f"No prepackaged downloadable items available for {data_id}")

        # This is to make sure that we do not send requests for currently for
        # unsupported datasets.

        full_source: str = (
            product.get(_DATASET_DOWNLOAD_INFORMATION_KEY)
            .get(_ITEMS_KEY)[0]
            .get(_FULL_SOURCE_KEY)
        )

        assert full_source not in _NOT_SUPPORTED_LIST or full_source is None, (
            f"This data product: {product[_TITLE_KEY]} is not yet supported in "
            f"this plugin yet."
        )

        # EEA datasets are the prepackaged datasets that are available for
        # download on request
        if full_source == "EEA":
            status, task_id = self.get_current_requests_status(
                dataset_id=product[_UID_KEY], file_id=item[_ID_KEY]
            )

            if status == COMPLETE or status == PENDING:
                LOG.debug(
                    f"Download request with task id {task_id} "
                    f"{'already completed' if status == 'COMPLETE' else 'is in queue'} for data id: {data_id}"
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
                data_id, item, product, full_source
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

        # In these other dataset sources (currently LEGACY is supported),
        # the CLMS API returns a list of links which we need to loop over and
        # download.
        elif full_source in SUPPORTED_NON_EEA_DATASET_SOURCES:
            download_request_url, headers = self._prepare_download_request(
                data_id, item, product, full_source
            )
            response_data = make_api_request(
                method="GET", url=download_request_url, headers=headers
            )
            response = get_response_of_type(response_data, "json")
            return response
        else:
            raise ValueError(
                f"The dataset: {data_id} from source: {full_source} is not supported"
            )

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

        url = build_api_url(self._url, TASK_STATUS_ENDPOINT)
        response_data = make_api_request(url=url, headers=headers)
        response = get_response_of_type(response_data, "json")
        try:
            task_info = response.get(task_id)
            status = task_info.get(_STATUS_KEY)
        except (AttributeError, ValueError, KeyError):
            return "", -1
        if status in _STATUS_COMPLETE:
            try:
                return (
                    task_info[_DOWNLOAD_URL_KEY],
                    task_info["FileSize"],
                )
            except KeyError:
                return "", -1
        else:
            raise Exception(
                f"Task ID {task_id} has not yet finished. "
                "No download url available yet."
            )

    def _prepare_download_request(
        self, data_id: str, item: dict, product: dict, source: str
    ) -> tuple[str, dict, dict] | tuple[str, dict]:
        """Prepares the API request details for downloading a dataset.

        Args:
            data_id: Unique identifier of the dataset.
            item: Metadata for the specific file to download.
            product: Metadata for the dataset containing the file.
            source: Unique identifier that indicates the source of the
                dataset which determines how the download request is prepared.

        Returns:
            tuple[str, dict, dict] | tuple[str, dict]: A tuple containing the
                request URL, headers, and/or JSON payload.
        """
        LOG.debug(f"Preparing download request for {data_id}")

        dataset_uid = product[_UID_KEY]
        file_id = item[_ID_KEY]

        headers = ACCEPT_HEADER.copy()
        headers.update(CONTENT_TYPE_HEADER)
        headers.update(get_authorization_header(self._api_token))
        if source == "EEA":
            url = build_api_url(self._url, DOWNLOAD_ENDPOINT)
            json = get_dataset_download_info(
                dataset_id=dataset_uid,
                file_id=file_id,
            )
            return url, headers, json
        else:
            extra_params = {
                "dataset_uid": dataset_uid,
                "download_information_id": file_id,
            }
            if source == "LEGACY":
                date_range = product[_CHARACTERISTICS_TEMPORAL_EXTENT]
                start_year, end_year = date_range.split("-")

                date_from = f"{start_year}-01-01"
                if end_year == "present":
                    date_to = datetime.today().strftime("%Y-%m-%d")
                else:
                    date_to = f"{end_year}-12-31"

                extra_params.update(
                    {
                        "date_from": date_from,
                        "date_to": date_to,
                    }
                )

            url = build_api_url(
                self._url, GET_DOWNLOAD_FILE_URLS_ENDPOINT, extra_params=extra_params
            )
            return url, headers

    def get_current_requests_status(
        self,
        dataset_id: str | None = None,
        file_id: str | None = None,
        task_id: str | None = None,
    ) -> tuple[str, str]:
        """Checks the status of existing download request task.

        You can either provide the dataset_id and file_id or just the
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
        if dataset_id:
            assert (
                file_id is not None
            ), "File ID is missing when dataset_id is provided."
            if task_id:
                LOG.warning(
                    "task_id provided will be ignored as dataset_id "
                    "and file_id are provided"
                )

        self._token_handler.refresh_token()
        headers = ACCEPT_HEADER.copy()
        headers.update(get_authorization_header(self._api_token))

        url = build_api_url(self._url, TASK_STATUS_ENDPOINT)
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

    def download_zip_data(self, download_url: str, data_id: str) -> None:
        """Downloads, extracts, and saves the dataset from the provided URL.

        Args:
            download_url: URL for downloading the dataset.
            data_id: Unique identifier of the dataset.
        """
        LOG.debug(f"Downloading zip file from {download_url}")
        print(f"Downloading zip file from {download_url}")

        response = make_api_request(download_url, timeout=600, stream=True)
        print("after api request", response)
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

                    geo_files = DownloadTaskManager._find_geo_in_dir(
                        "/",
                        inner_zip_fs,
                    )
                    if geo_files:
                        target_folder = self.cache_store.fs.sep.join(
                            [self.cache_store.root, DOWNLOAD_FOLDER, data_id]
                        )
                        self.cache_store.fs.makedirs(
                            target_folder,
                            exist_ok=True,
                        )
                        for geo_file in geo_files:
                            try:
                                with inner_zip_fs.open(geo_file, "rb") as source_file:
                                    geo_file_name = geo_file.split("/")[-1]
                                    geo_file_path = self.cache_store.fs.sep.join(
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
                                LOG.error(
                                    f"Error occurred while reading/writing data. {e}"
                                )
                                raise
                            except Exception as e:
                                LOG.error(f"An unexpected error occurred: {e}")
                                raise

                    else:
                        raise FileNotFoundError(
                            "No file found in the downloaded zip file to load"
                        )

    def download_file(self, url: str, data_id: str):
        _file = url.split("/")[-1]
        dir_path = self.fs.sep.join([self.download_folder, data_id])
        filename = self.fs.sep.join([dir_path, _file])
        if not self.fs.exists(dir_path):
            self.fs.makedirs(dir_path, exist_ok=True)
        if not self.fs.isfile(filename):
            for attempt in range(1, _MAX_RETRIES + 1):
                try:
                    response = make_api_request(url, timeout=600, stream=True)
                    if response.status_code == 200:
                        with self.fs.open(filename, "wb") as f:
                            for chunk in response.iter_content(chunk_size=1024 * 1024):
                                f.write(chunk)
                        break

                except (RequestException, IOError) as e:
                    LOG.error(f"Error downloading {url}: {e}")
                    wait_time = 2**attempt
                    LOG.debug(
                        f"Attempt {attempt}. Waiting {wait_time} seconds before retrying..."
                    )
                    time.sleep(wait_time)

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
                in `_GEO_FILE_EXTS`.
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
