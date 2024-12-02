import glob
import io
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures._base import CANCELLED

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
    ID_KEY,
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
    ORIGINAL_FILENAME_KEY,
    FILE_ID_KEY,
    RETRY_TIMEOUT,
    STATUS_CANCELLED,
    EXPIRED,
    STATUS_REJECTED,
    REJECTED,
)
from xcube_clms.preload_handle import PreloadHandle
from xcube_clms.preloadtask import PreloadTask
from xcube_clms.utils import (
    make_api_request,
    get_response_of_type,
    get_authorization_header,
    get_dataset_download_info,
    build_api_url,
    has_expired,
    find_geo_in_dir,
)


class PreloadData:
    def __init__(self, url, credentials, path):
        self._credentials: dict = {}
        self._api_token: str = None
        self._clms_api_token_instance = None
        self._url: str = url
        self.tasks: list[PreloadTask] = []
        self.path: str = path

        self._set_credentials(credentials)

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

        status, task_id = self._get_current_requests_status(
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

    def initiate_preload(self, data_id_maps: dict):
        executor = ThreadPoolExecutor()

        for data_id_map in data_id_maps.items():
            executor.submit(self._initiate_preload, self.path, data_id_map)

        preload_handle = PreloadHandle(self.tasks)

        # TODO: Update the status of various steps in the process and check
        #  for the right thing here.
        def update_ui_periodically():
            while any(task.download_status != "Success" for task in self.tasks):
                preload_handle.update_html()
                time.sleep(1)

        # Start the UI update thread
        ui_thread = threading.Thread(target=update_ui_periodically)
        ui_thread.daemon = True
        ui_thread.start()

        return preload_handle

    @staticmethod
    def _is_data_cached(data_id, path):
        if not os.path.isdir(path):
            return False
        if len(glob.glob(f"{os.path.join(path, data_id)}.*")) != 1:
            return False
        return True

    def _initiate_preload(self, path, data_id_map: tuple[str:dict]):
        data_id = data_id_map[0]
        task_id = self.request_download(
            data_id=data_id,
            item=data_id_map[1].get("item"),
            product=data_id_map[1].get("product"),
        )

        task = None
        for _task in self.tasks:
            if task_id == _task.task_id:
                task = _task
                break

        if task is None:
            task = PreloadTask(
                data_id,
                task_id,
                self._url,
                self._api_token,
            )
            task.update_html()
            self.tasks.append(task)

        task_events = task.get_events()
        cancel_event = task_events.get("cancel_event")
        queue_event = task_events.get("queue_event")

        # spinner_thread = threading.Thread(
        #     target=task.spinner, args=(queue_event, task_id, cancel_event)
        # )

        # spinner_thread.start()

        # Check if the data is already downloaded and extracted at the given
        # path
        if self._is_data_cached(data_id, path):
            # spinner_thread.join()
            LOG.info(f"The data for {data_id} is already cached at {path}")
            return

        # Set the queue event to indicate that the queue process has started.
        queue_event.set()
        task.update_html()

        while queue_event.is_set():
            # Check if the cancel_event was set, if so clear the queue and
            # return
            if cancel_event.is_set():
                queue_event.clear()
                task.update_html()
                # spinner_thread.join()
                return

            status, _ = self._get_current_requests_status(task_id=task_id)
            if status == COMPLETE:
                # Clear the queue event and stop the spinner.
                queue_event.clear()
                # spinner_thread.join()
                download_event = task_events.get("download_event")
                download_event.set()
                task.update_html()
                download_url = self._get_download_url(task_id)
                # Use the path used to create a filestore.
                extraction_event = task_events.get("extraction_event")
                self._download_and_extract_data(
                    download_url, path, task_id, extraction_event
                )
                download_event.clear()
                task.update_html()
            time.sleep(RETRY_TIMEOUT)

    def _get_download_url(self, task_id):
        """
        :param task_id: ID of the task that is requested for the dataset
        :return: Download URL string
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

    def _prepare_download_request(
        self, data_id: str, item: dict, product: dict
    ) -> tuple[str, dict, dict]:
        LOG.info(f"Preparing download request for {data_id}")

        dataset_id = product[UID_KEY]
        file_id = item[ID_KEY]
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
        self,
        dataset_id: str | None = None,
        file_id: str | None = None,
        task_id: str | None = None,
    ) -> tuple[str, str]:
        """
        If all dataset_id, file_id and task_id are provided, task_id is ignored.
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
            requested_file_id = datasets[0][FILE_ID_KEY]
            condition = (
                ((dataset_id == requested_data_id) and (file_id == requested_file_id))
                if (dataset_id and file_id)
                else (key == task_id)
            )
            if status in STATUS_PENDING and condition:
                return PENDING, key
            if status == STATUS_COMPLETE and condition:
                if has_expired(response[key][DOWNLOAD_AVAILABLE_TIME_KEY]):
                    return EXPIRED, ""
                else:
                    return COMPLETE, key
            if status == STATUS_CANCELLED and condition:
                return CANCELLED, key
            if status == STATUS_REJECTED and condition:
                return REJECTED, key

        return UNDEFINED, ""

    @staticmethod
    def _download_and_extract_data(download_url, path, task_id, extraction_event):
        LOG.info(f"Downloading zip file from {download_url}")
        download_progress_bar = tqdm(
            total=100,
            desc=f"Downloading zip file for task: {task_id}",
            unit_scale=True,
        )

        response_data = make_api_request(download_url, stream=True)
        response = get_response_of_type(response_data, BYTES_TYPE)

        total_size = float(response.headers.get("content-length", 0))
        chunk_size = 1024 * 1024

        extraction_event.set()
        with io.BytesIO(response.content) as zip_file_in_memory:
            for chunk in response.iter_content(chunk_size=chunk_size):
                zip_file_in_memory.write(chunk)
                bar_update = (len(chunk) / total_size) * 100
                download_progress_bar.update(bar_update)
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
                extraction_event.clear()
                return
            else:
                LOG.warn("No downloadable zip file found inside.")
                extraction_event.clear()
                return

            LOG.info(
                f"Found one zip file " f"{actual_zip_file.get(ORIGINAL_FILENAME_KEY)}."
            )
            with fs.open(actual_zip_file[NAME_KEY], "rb") as f:
                zip_fs = fsspec.filesystem("zip", fo=f)

                geo_file = find_geo_in_dir(
                    "/",
                    zip_fs,
                )
                if geo_file:
                    try:
                        file_path = os.path.join(path, geo_file)
                        with zip_fs.open(geo_file, "rb") as source_file:
                            with open(file_path, "wb") as dest_file:
                                dest_file.write(source_file.read())
                                extraction_event.clear()
                        LOG.info(
                            f"The file {geo_file} has been successfully "
                            f"downloaded to {file_path}"
                        )

                    except OSError as e:
                        extraction_event.clear()
                        raise OSError(f"Error occurred while writing data. {e}")
                    except UnicodeDecodeError as e:
                        extraction_event.clear()
                        raise ValueError(
                            f"Decoding error: {e}. File might not be text "
                            f"or encoding is incorrect."
                        )
                    except Exception as e:
                        extraction_event.clear()
                        raise Exception(f"An unexpected error occurred: {e}")

                else:
                    extraction_event.clear()
                    raise FileNotFoundError(
                        "No file found in the downloaded zip file to load"
                    )
