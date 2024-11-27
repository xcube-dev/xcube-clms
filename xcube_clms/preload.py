import io
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from itertools import cycle

import fsspec
from tqdm.notebook import tqdm

from xcube_clms.api_token import CLMSAPIToken
from xcube_clms.constants import (
    JSON_TYPE,
    LOG,
    UNDEFINED,
    COMPLETE,
    TITLE_KEY,
    NOT_SUPPORTED_LIST,
    FULL_SOURCE_KEY,
    ITEMS_KEY,
    DATASET_DOWNLOAD_INFORMATION_KEY,
    SOURCE_KEY,
    PATH_KEY,
    UID_KEY,
    DOWNLOAD_ENDPOINT,
    FILE_ID_KEY,
    ACCEPT_HEADER,
    CONTENT_TYPE_HEADER,
    TASK_STATUS_ENDPOINT,
    STATUS_KEY,
    DATASETS_KEY,
    DATASET_ID_KEY,
    STATUS_PENDING,
    PENDING,
    STATUS_COMPLETE,
    DOWNLOAD_URL_KEY,
    RESULTS,
    FILENAME_KEY,
    NAME_KEY,
    BYTES_TYPE,
    TASK_IDS_KEY,
    TASK_ID_KEY,
    DOWNLOAD_AVAILABLE_TIME_KEY,
)
from xcube_clms.utils import (
    make_api_request,
    get_response_of_type,
    get_authorization_header,
    get_dataset_download_info,
    build_api_url,
    has_expired,
)


class PreloadData:
    def __init__(self, url, credentials, path):
        self._credentials: dict = {}
        self._api_token = None
        self._clms_api_token_instance = None
        self._url = url
        self.cancel_event = threading.Event()
        if path is None:
            self.path = os.getcwd()
        else:
            self.path = path
        if credentials:
            self._credentials = credentials
            self._clms_api_token_instance = CLMSAPIToken(credentials=credentials)
            self._api_token: str = self._clms_api_token_instance.access_token

    def _set_credentials(self, credentials: dict):
        self._credentials = credentials
        self._clms_api_token_instance = CLMSAPIToken(credentials=credentials)
        self._api_token: str = self._clms_api_token_instance.access_token

    def _refresh_token(self):
        if not self._api_token or self._clms_api_token_instance.is_token_expired():
            LOG.info("Token expired or not present. Refreshing token.")
            self._api_token = self._clms_api_token_instance.refresh_token()
        else:
            LOG.info("Current token valid. Reusing it.")

    def request_download(self, data_id: str, item: dict, product: dict) -> str:
        self._refresh_token()

        # This is to make sure that there are pre-packaged files available for
        # download. Without this, the API throws the following error: Error,
        # the FileID is not valid.
        # We check for path and source based on the API code here:
        # https://github.com/eea/clms.downloadtool/blob/master/clms/downloadtool/api/services/datarequest_post/post.py#L177-L196

        path = item.get(PATH_KEY, "")
        source = item.get(SOURCE_KEY, "")

        if (path is "") and (source is ""):
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

        status, task_id = self._get_current_requests_status(dataset_id=product[UID_KEY])
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

    def process_tasks(self, task_ids):
        with ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(
                    self._check_status_and_download,
                    task_ids[task_id][TASK_ID_KEY],
                    self.path,
                ): task_id
                for task_id in task_ids
            }

            for future in futures:
                task_id = futures[future]
                try:
                    result = future.result()
                except Exception as e:
                    """Handle it"""

    def _check_status_and_download(self, task_id, path):
        status_event = threading.Event()
        spinner_thread = threading.Thread(
            target=self.spinner, args=(status_event, task_id)
        )
        spinner_thread.start()
        status_event.set()

        while status_event.is_set():
            spinner_thread.join()
            status, _ = self._get_current_requests_status(task_id=task_id)
            if status == COMPLETE:
                status_event.clear()
                download_url = self._get_download_url(task_id)
                # Use the path used to create a filestore.
                self._download_data(download_url, path, task_id)
            time.sleep(60)

    def _get_download_url(self, task_id):
        """
        Implement me!
        :param task_id:
        :return:
        """
        self._refresh_token()

        headers = ACCEPT_HEADER.copy()
        headers.update(get_authorization_header(self._api_token))

        url = build_api_url(self._url, TASK_STATUS_ENDPOINT, datasets_request=False)
        response_data = make_api_request(url=url, headers=headers)
        response = get_response_of_type(response_data, JSON_TYPE)

        for key in response:
            if key == task_id:
                status = response[key][STATUS_KEY]
                if status in STATUS_COMPLETE:
                    return response[key][DOWNLOAD_URL_KEY]
                else:
                    raise Exception(
                        f"Task ID {task_id} has not yet finished. No download url available yet."
                    )

    @staticmethod
    def spinner(event, task_id):
        """
        Displays a spinner with elapsed time for a single task until the event is set.
        """
        spinner = cycle(["◐", "◓", "◑", "◒"])
        start_time = time.time()
        while event.is_set():
            elapsed = int(time.time() - start_time)
            print(
                f"\rTask {task_id}: {next(spinner)} Elapsed time: {elapsed}s",
                end="",
                flush=True,
            )
            time.sleep(0.1)
        print(f"\rTask {task_id}: Done!{' ' * 20}")

    def _prepare_download_request(
        self, data_id: str, item: dict, product: dict
    ) -> tuple[str, dict, dict]:
        LOG.info(f"Preparing download request for {data_id}")

        dataset_id = product[UID_KEY]
        file_id = item[FILE_ID_KEY]
        json = get_dataset_download_info(
            dataset_id=dataset_id,
            file_id=file_id,
        )
        url = build_api_url(self._url, DOWNLOAD_ENDPOINT, datasets_request=False)
        if not self._api_token:
            self._refresh_token()
        headers = ACCEPT_HEADER.copy()
        headers.update(CONTENT_TYPE_HEADER)
        headers.update(get_authorization_header(self._api_token))

        return url, headers, json

    def _get_current_requests_status(
        self, dataset_id: str | None = None, task_id: str | None = None
    ) -> tuple[str, str]:
        """
        If both dataset_id and task_id are provided, task_id is ignored.
        """
        self._refresh_token()

        headers = ACCEPT_HEADER.copy()
        headers.update(get_authorization_header(self._api_token))

        url = build_api_url(self._url, TASK_STATUS_ENDPOINT, datasets_request=False)
        response_data = make_api_request(url=url, headers=headers)
        response = get_response_of_type(response_data, JSON_TYPE)

        for key in response:
            status = response[key][STATUS_KEY]
            datasets = response[key][DATASETS_KEY]
            requested_data_id = datasets[0][DATASET_ID_KEY]
            condition = (
                (dataset_id == requested_data_id) if dataset_id else (key == task_id)
            )

            if status in STATUS_PENDING and condition:
                return PENDING, key
            if status in STATUS_COMPLETE and condition:
                if has_expired(response[key][DOWNLOAD_AVAILABLE_TIME_KEY]):
                    return UNDEFINED, ""
                else:
                    return COMPLETE, key

        return UNDEFINED, ""

    @staticmethod
    def _download_data(download_url, path, task_id):
        LOG.info(f"Downloading zip file from {download_url}")
        download_progress_bar = tqdm(
            total=100, desc=f"Downloading zip file for task: {task_id}"
        )
        headers = ACCEPT_HEADER.copy()
        headers.update(CONTENT_TYPE_HEADER)
        response_data = make_api_request(download_url, headers=headers, stream=True)
        response = get_response_of_type(response_data, BYTES_TYPE)

        total_size = int(response.headers.get("content-length", 0))
        chunk_size = 1024 * 1024
        downloaded_size = 0

        with io.BytesIO(response.content) as zip_file_in_memory:
            for chunk in response.iter_content(chunk_size=chunk_size):
                zip_file_in_memory.write(chunk)
                downloaded_size += len(chunk)
                download_progress_bar.update(int(len(chunk) / total_size * 100))
            download_progress_bar.close()

            zip_file_in_memory.seek(0)
            fs = fsspec.filesystem("zip", fo=zip_file_in_memory)
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
                LOG.info(f"Found one zip file {actual_zip_file}.")
                with fs.open(actual_zip_file[NAME_KEY], "rb") as f:
                    zip_fs = fsspec.filesystem("zip", fo=f)

                    geo_file = PreloadData._find_geo_in_dir(
                        "/",
                        zip_fs,
                    )
                    if geo_file:
                        target_file_path = os.path.join(path, geo_file)
                        os.makedirs(os.path.dirname(target_file_path), exist_ok=True)
                        try:
                            with zip_fs.open(geo_file, "rb") as source_file:
                                with open(target_file_path, "wb") as dest_file:
                                    dest_file.write(source_file.read())
                            LOG.info(
                                f"The file {geo_file} has successfully been downloaded to {target_file_path}"
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
    def _find_geo_in_dir(path, zip_fs):
        geo_file: str = ""
        contents = zip_fs.ls(path)
        for item in contents:
            if zip_fs.isdir(item[NAME_KEY]):
                geo_file = PreloadData._find_geo_in_dir(
                    item[NAME_KEY],
                    zip_fs,
                )
                if geo_file:
                    return geo_file
            else:
                if item[NAME_KEY].endswith(".tif"):
                    LOG.info(f"Found TIFF file: {item[NAME_KEY]}")
                    geo_file = item["name"]
                    return geo_file
                if item[NAME_KEY].endswith(".nc"):
                    LOG.info(f"Found NetCDF file: {item[NAME_KEY]}")
                    geo_file = item[NAME_KEY]
                    return geo_file
        return geo_file
