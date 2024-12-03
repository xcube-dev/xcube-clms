import io
import os
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures._base import CANCELLED
from typing import Any

import fsspec
import xarray as xr
from rioxarray import rioxarray
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
    GEO_FILE_EXTS,
    DATA_ID_SEPARATOR,
)
from xcube_clms.utils import (
    make_api_request,
    get_response_of_type,
    get_authorization_header,
    get_dataset_download_info,
    build_api_url,
    has_expired,
    spinner,
    find_easting_northing,
)


class PreloadData:
    def __init__(self, url, credentials, path):
        self._credentials: dict = {}
        self._api_token: str = None
        self._clms_api_token_instance = None
        self._url: str = url
        self._task_control = {}
        self.path: str = path
        self.cleanup: bool = False

        if credentials:
            self._set_credentials(credentials)

        self._cache: dict[str, str] = {}
        self._init_cache()

    def _init_cache(self):
        self._cache = {
            d: os.path.join(self.path, d)
            for d in os.listdir(self.path)
            if DATA_ID_SEPARATOR in d
        }

    def _update_cache(self, path, data_id, file_name):
        self._cache.update({data_id: os.path.join(path, data_id, file_name)})

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

    def initiate_preload(self, data_id_maps: dict[str, Any]):
        self._update_cache(self.path)

        # We create a status event for each of the tasks so that we can
        # signal the thread that it can proceed further from the queue.
        for data_id_map_key in data_id_maps.keys():
            self._task_control[data_id_map_key] = {
                "status_event": threading.Event(),
            }

        executor = ThreadPoolExecutor()
        for data_id_map in data_id_maps.items():
            executor.submit(
                self._initiate_preload,
                data_id_map,
                self._task_control[data_id_map[0]]["status_event"],
            )

    def _initiate_preload(self, data_id_map, status_event):
        data_id = data_id_map[0]
        if data_id in self._cache.keys():
            LOG.info(f"The data for {data_id} is already cached at {self.path}")
            return

        task_id = self.request_download(
            data_id=data_id,
            item=data_id_map[1].get("item"),
            product=data_id_map[1].get("product"),
        )

        spinner_thread = threading.Thread(
            target=spinner, args=(status_event, f"{task_id}")
        )
        status_event.set()
        spinner_thread.start()

        while status_event.is_set():
            status, _ = self._get_current_requests_status(task_id=task_id)
            if status == COMPLETE:
                status_event.clear()
                spinner_thread.join()
                download_url = self._get_download_url(task_id)
                self._download_data(download_url, task_id, data_id)
                self._postprocess(data_id)
            time.sleep(RETRY_TIMEOUT)

    def _postprocess(self, data_id):
        files = os.listdir(os.path.join(self.path, data_id))
        if len(files) == 1:
            LOG.info("No postprocessing required.")
        elif len(files) == 0:
            LOG.warn("No files to postprocess!")
        else:
            new_filename = data_id.split(DATA_ID_SEPARATOR)[-1] + ".zarr"
            en_map = {}
            for file in files:
                en = find_easting_northing(file)
                en_map.update({file: en})
            if en_map is {}:
                LOG.error("This naming format is not supported")
            self._merge_and_save(en_map)
            if self.cleanup:
                self._cleanup_dir()

    def _merge_and_save(self, en_map):
        grouped_files = defaultdict(list)
        for file, coords in en_map.items():
            grouped_files[coords].append(file)

        # Step 1: Group by Easting
        east_groups = defaultdict(list)
        for coord, file_list in grouped_files.items():
            easting = coord[:3]
            east_groups[easting].extend(file_list)

        # Step 2: Merge files along the Y-axis (Northing) for each Easting group
        merged_eastings = {}
        for easting, file_list in east_groups.items():
            datasets = []
            for file in file_list:
                da = rioxarray.open_rasterio(file, masked=True)
                datasets.append(da)
            merged_eastings[easting] = xr.concat(datasets, dim="y")

        # Step 3: Merge along the X-axis (Easting)
        final_datasets = list(merged_eastings.values())
        final_cube = xr.concat(final_datasets, dim="x")

        # TODO: Save to a zarr file. Check for kernel death issues

    def _cleanup_dir(self): ...

    def _get_download_url(self, task_id):
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
            requested_file_id = datasets[0][FILE_ID_KEY]
            condition = (
                ((dataset_id == requested_data_id) and (file_id == requested_file_id))
                if dataset_id
                else (key == task_id)
            )
            if status in STATUS_PENDING and condition:
                return PENDING, key
            if status in STATUS_COMPLETE and condition:
                if has_expired(response[key][DOWNLOAD_AVAILABLE_TIME_KEY]):
                    return UNDEFINED, ""
                else:
                    return COMPLETE, key
            if status in STATUS_CANCELLED and condition:
                return CANCELLED, key

        return UNDEFINED, ""

    def _download_data(self, download_url, task_id, data_id):
        LOG.info(f"Downloading zip file from {download_url}")

        response_data = make_api_request(download_url, timeout=400, stream=True)
        response = get_response_of_type(response_data, BYTES_TYPE)

        content_length = float(response.headers.get("content-length", 0))
        chunk_size = 1024 * 1024  # 1 MB chunks
        total_size = float(content_length)
        print("total size::", total_size)

        download_progress_bar = tqdm(
            unit="B",
            desc=f"Downloading zip file for task: {task_id}",
            unit_scale=True,
        )

        with io.BytesIO(response.content) as zip_file_in_memory:
            for chunk in response.iter_content(chunk_size=chunk_size):
                zip_file_in_memory.write(chunk)
                download_progress_bar.update(len(chunk))
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
                LOG.info(
                    f"Found one zip file "
                    f"{actual_zip_file.get(ORIGINAL_FILENAME_KEY)}."
                )
                with fs.open(actual_zip_file[NAME_KEY], "rb") as f:
                    zip_fs = fsspec.filesystem("zip", fo=f)

                    geo_files = PreloadData._find_geo_in_dir(
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
    def _find_geo_in_dir(path, zip_fs):
        geo_file: list[str] = []
        contents = zip_fs.ls(path)
        for item in contents:
            if zip_fs.isdir(item[NAME_KEY]):
                geo_file.extend(
                    PreloadData._find_geo_in_dir(
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
