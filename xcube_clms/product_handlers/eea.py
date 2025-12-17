import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta
from math import ceil
from typing import Any, Container, Iterator

import rasterio
import rioxarray
import xarray as xr
from xcube.core.chunk import chunk_dataset
from xcube.core.store import (
    DataStoreError,
    PreloadHandle,
    PreloadState,
    DataTypeLike,
    DataDescriptor,
    DatasetDescriptor,
    DataStore,
)
from xcube.util.jsonschema import JsonObjectSchema

from xcube_clms.api_token_handler import ClmsApiTokenHandler
from xcube_clms.constants import (
    ACCEPT_HEADER,
    CANCELLED,
    CLMS_API_URL,
    COMPLETE,
    CONTENT_TYPE_HEADER,
    DATA_ID_SEPARATOR,
    DOWNLOAD_ENDPOINT,
    DOWNLOAD_FOLDER,
    ID_KEY,
    ITEM_KEY,
    LOG,
    PENDING,
    PRODUCT_KEY,
    RETRY_TIMEOUT,
    TASK_STATUS_ENDPOINT,
    TIME_TO_EXPIRE,
    UID_KEY,
    CLMS_DATA_ID_KEY,
    DOWNLOADABLE_FILES_KEY,
    ITEMS_KEY,
    FORMAT_KEY,
    FILE_KEY,
    CRS_KEY,
)
from xcube_clms.preload import ClmsPreloadHandle
from xcube_clms.product_handler import ProductHandler
from xcube_clms.utils import (
    build_api_url,
    cleanup_dir,
    find_easting_northing,
    get_authorization_header,
    get_dataset_download_info,
    get_extracted_component,
    get_response_of_type,
    get_tile_size,
    make_api_request,
    normalize_time_range,
    download_zip_data,
)

_FILE_ID_KEY = "FileID"
_DOWNLOAD_URL_KEY = "DownloadURL"
_STATUS_KEY = "Status"
_DATASETS_KEY = "Datasets"
_DATASET_ID_KEY = "DatasetID"
_PATH_KEY = "path"
_SOURCE_KEY = "source"
_TASK_IDS_KEY = "TaskIds"
_TASK_ID_KEY = "TaskID"
_DOWNLOAD_AVAILABLE_TIME_KEY = "FinalizationDateTime"
_STATUS_PENDING = ["Queued", "In_progress"]
_STATUS_COMPLETE = ["Finished_ok"]
_STATUS_CANCELLED = ["Cancelled"]
_UNDEFINED = "UNDEFINED"
_ZARR_FORMAT = ".zarr"
_START_TIME_KEY = "temporalExtentStart"
_END_TIME_KEY = "temporalExtentEnd"


class EeaProductHandler(ProductHandler):
    """
    Implements support for EEA pre-packaged products available via the CLMS API.

    These products are provided as a ZIP archive containing ZIP files,
    which can be downloaded through a link received after submitting a dataset
    request.
    """

    def __init__(
        self,
        cache_store: DataStore = None,
        datasets_info: list[dict] = None,
        api_token_handler: ClmsApiTokenHandler = None,
    ):
        super().__init__(cache_store, datasets_info, api_token_handler)

        self.data_id_maps = None
        self._api_token = self.api_token_handler.api_token
        self.fs = self.cache_store.fs
        self.download_folder = self.fs.sep.join(
            [self.cache_store.root, DOWNLOAD_FOLDER]
        )
        self.cleanup = None
        self.tile_size = None

    @classmethod
    def product_type(cls):
        return "eea"

    def get_open_data_params_schema(self, data_id: str = None) -> JsonObjectSchema:
        return self.cache_store.get_open_data_params_schema(data_id)

    def get_data_id(
        self,
        data_type: DataTypeLike = None,
        include_attrs: Container[str] | bool = False,
        item: dict = None,
    ) -> Iterator[str | tuple[str, dict[str, Any]]]:
        for i in item[DOWNLOADABLE_FILES_KEY][ITEMS_KEY]:
            if FORMAT_KEY in i and i[FORMAT_KEY] == "Geotiff":
                if FILE_KEY in i and i[FILE_KEY] != "":
                    data_id = (
                        f"{item[CLMS_DATA_ID_KEY]}{DATA_ID_SEPARATOR}{i[FILE_KEY]}"
                    )
                    if not include_attrs:
                        yield data_id
                    elif isinstance(include_attrs, bool) and include_attrs:
                        yield data_id, i
                    elif isinstance(include_attrs, list):
                        filtered_attrs = {
                            attr: i[attr] for attr in include_attrs if attr in i
                        }
                        yield data_id, filtered_attrs

    def describe_data(self, data_id: str, product: dict) -> DataDescriptor:
        crs = product.get(CRS_KEY, [])
        time_range = (product.get(_START_TIME_KEY), product.get(_END_TIME_KEY))
        normalized_time_range = normalize_time_range(time_range)
        if len(crs) > 1:
            LOG.warning(
                f"Expected 1 crs, got {len(crs)}. Outputting the first element."
            )
        metadata = dict(time_range=normalized_time_range, crs=crs[0] if crs else None)
        return DatasetDescriptor(data_id, **metadata)

    def open_data(
        self,
        data_id: str,
        **open_params,
    ) -> Any:
        """Opens and returns data for a given data ID from the cache.

        Args:
            data_id : Identifier for the dataset.
            **open_params: Additional parameters, including opener_id.

        Returns:
            Any: The opened dataset.

        Raises:
            DataStoreError: If the data is not cached and needs to be
            preloaded first.
        """
        if not self.cache_store.has_data(data_id):
            raise DataStoreError(
                f"No cached data found for data_id: "
                f"{data_id}. Please preload the data "
                f"first using the `preload_data()` method."
            )
        opener_id = open_params.get("opener_id")
        return self.cache_store.open_data(
            data_id=data_id, opener_id=opener_id, **open_params
        )

    def preload_data(
        self,
        *data_ids: str,
        **preload_params: Any,
    ) -> PreloadHandle:
        """Initiates the data preload process for one or more datasets.

        Args:
            *data_ids : One or more dataset IDs to preload.
            **preload_params : Additional parameters.

        Returns:
            PreloadHandle: The preload handle to track and control the process.
        """
        self.cleanup = preload_params.get("cleanup", True)
        tile_size = preload_params.get("tile_size", None)
        self.tile_size = get_tile_size(tile_size)
        self.data_id_maps = {
            data_id: {
                ITEM_KEY: get_extracted_component(
                    self.datasets_info, data_id, item_type="item"
                ),
                PRODUCT_KEY: get_extracted_component(self.datasets_info, data_id),
            }
            for data_id in data_ids
        }
        # noinspection PyPropertyAccess
        self.cache_store.preload_handle = ClmsPreloadHandle(
            data_id_maps=self.data_id_maps,
            url=CLMS_API_URL,
            cache_store=self.cache_store,
            preload_data=self._preload_data,
            **preload_params,
        )
        return self.cache_store

    def _preload_data(self, handle, data_id):
        """Handles the full lifecycle of preloading a single dataset.

        This includes initiating the download request, monitoring task status,
        downloading and extracting data, and preprocessing it.

        Args:
            handle (PreloadHandle): The preload handle for managing updates
            and status.
            data_id (str): The identifier for the dataset to preload.
        """
        status_event = threading.Event()

        if DATA_ID_SEPARATOR in data_id:
            task_id = self.request_download(
                data_id=data_id,
            )[0]

            handle.notify(
                PreloadState(
                    data_id=data_id,
                    progress=0.1,
                    message=f"Task ID {task_id}: Download request in queue.",
                )
            )
            status_event.set()

            while status_event.is_set():
                status, _ = self._get_current_requests_status(task_id=task_id)
                if status == COMPLETE:
                    status_event.clear()
                    handle.notify(
                        PreloadState(
                            data_id=data_id,
                            progress=0.4,
                            message=f"Task ID {task_id}: Download link "
                            f"created. "
                            f"Downloading and extracting now...",
                        )
                    )
                    download_url, _ = self._get_download_url(task_id)
                    download_zip_data(self.cache_store, download_url, data_id)
                    handle.notify(
                        PreloadState(
                            data_id=data_id,
                            progress=0.8,
                            message=f"Task ID {task_id}: Extraction complete. "
                            f"Processing now...",
                        )
                    )
                    self.preprocess_data(data_id)
                    handle.notify(
                        PreloadState(
                            data_id=data_id,
                            progress=1.0,
                            message=f"Task ID {task_id}: Preloading Complete.",
                        )
                    )
                    return
                if status in CANCELLED:
                    status_event.clear()
                    handle.notify(
                        PreloadState(
                            data_id=data_id,
                            message=f"Task ID {task_id}: Download request was "
                            f"cancelled by the user from "
                            "the Land Copernicus UI.",
                        )
                    )
                    handle.cancel()
                    return

                time.sleep(RETRY_TIMEOUT)

    def request_download(self, data_id) -> list[str]:
        self.api_token_handler.refresh_token()
        item = get_extracted_component(
            datasets_info=self.datasets_info, data_id=data_id, item_type="item"
        )
        product = get_extracted_component(
            datasets_info=self.datasets_info, data_id=data_id
        )
        # This is to make sure that there are pre-packaged files available for
        # download. Without this, the API throws the following error: Error,
        # the FileID is not valid.
        # We check for path and source based on the API code here:
        # https://github.com/eea/clms.downloadtool/blob/master/clms
        # /downloadtool/api/services/datarequest_post/post.py#L177-L196

        path = item.get(_PATH_KEY, "")
        source = item.get(_SOURCE_KEY, "")

        if (path == "") and (source == ""):
            LOG.info(f"No prepackaged downloadable items available for {data_id}")

        status, task_id = self._get_current_requests_status(data_id=data_id)

        if status == COMPLETE or status == PENDING:
            LOG.debug(
                f"Download request with task id {task_id} "
                f"{'already completed' if status == 'COMPLETE' else 'is in queue'} for data id: {data_id}"
            )
            return [task_id]
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

        download_request_url, headers = self.prepare_request(data_id)
        json = get_dataset_download_info(
            dataset_id=product[UID_KEY],
            file_id=item[ID_KEY],
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
        return [task_id]

    def prepare_request(
        self, data_id: str
    ) -> tuple[str, dict, dict] | tuple[str, dict]:
        LOG.debug(f"Preparing download request for {data_id}")

        headers = ACCEPT_HEADER.copy()
        headers.update(CONTENT_TYPE_HEADER)
        headers.update(get_authorization_header(self._api_token))
        url = build_api_url(CLMS_API_URL, DOWNLOAD_ENDPOINT)

        return url, headers

    def _get_download_url(self, task_id: str) -> tuple[str, int]:
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
        self.api_token_handler.refresh_token()

        headers = ACCEPT_HEADER.copy()
        headers.update(get_authorization_header(self._api_token))

        url = build_api_url(CLMS_API_URL, TASK_STATUS_ENDPOINT)
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

    def _get_current_requests_status(
        self,
        data_id: str | None = None,
        task_id: str | None = None,
    ) -> tuple[str, str]:
        """Checks the status of existing download request task.

        Either provide the data_id or the task_id to enquire the
        status of the request. If task_id is provided, data_id will be ignored.

        The sorting is performed based on the priority and timestamps so that
        we have the result of the latest requests in the decreasing order of
        priorities.

        Args:
            data_id: Data ID of the data to be requested (optional).
            task_id: Task ID of the requested dataset (optional).

        Returns:
            tuple[str, str]: A tuple containing the status
                (e.g., COMPLETE, PENDING) and task ID.
        """
        dataset_id = None
        file_id = None

        if data_id and not task_id:
            item = get_extracted_component(
                datasets_info=self.datasets_info, data_id=data_id, item_type="item"
            )
            product = get_extracted_component(
                datasets_info=self.datasets_info, data_id=data_id
            )

            dataset_id = product[UID_KEY]
            file_id = item[ID_KEY]

        self.api_token_handler.refresh_token()
        headers = ACCEPT_HEADER.copy()
        headers.update(get_authorization_header(self._api_token))

        url = build_api_url(CLMS_API_URL, TASK_STATUS_ENDPOINT)
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
            if task_id:
                condition = key == task_id
            else:
                condition = (dataset_id == requested_data_id) and (
                    file_id == requested_file_id
                )

            if condition:
                current_entry = {
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

    def preprocess_data(self, data_id: str, **preprocess_params) -> None:
        """Performs preprocessing on the files for a given data ID.

        This includes preparing files for merging, merging them based on their
        Easting and Northing coordinates computed from their file names,
        saving the merged file as a `.zarr` file, and optionally cleaning up
        the directory.

        We currently assume that all the datasets that are downloaded which
        contain multiple files will have this information in their
        file_names. This can be further improved once we find cases otherwise.

        Args:
            data_id: The identifier for the dataset being pre-processed.
        """

        target_folder = self.fs.sep.join([self.download_folder, data_id])
        files = [entry.split("/")[-1] for entry in self.fs.ls(target_folder)]
        if len(files) == 1:
            LOG.debug("Converting the file to zarr format.")
            cache_data_id = self.fs.sep.join([DOWNLOAD_FOLDER, data_id, files[0]])
            data = self.cache_store.open_data(cache_data_id, data_type="dataset")
            new_cache_data_id = data_id + _ZARR_FORMAT
            data = chunk_dataset(
                data,
                chunk_sizes={"x": self.tile_size[0], "y": self.tile_size[1]},
                format_name=_ZARR_FORMAT,
            )
            final_cube = data.rename(
                dict(band_1=f"{data_id.split(DATA_ID_SEPARATOR)[-1]}")
            )

            for var in final_cube.data_vars:
                if "grid_mapping" in final_cube[var].encoding:
                    del final_cube[var].encoding["grid_mapping"]

            self.cache_store.write_data(final_cube, new_cache_data_id, replace=True)
        elif len(files) == 0:
            LOG.warn("No files to preprocess!")
        else:
            en_map = self._prepare_merge(files, data_id)
            if not en_map:
                raise ValueError(
                    "This naming format is not supported. Currently "
                    "only filenames with Eastings and Northings are "
                    "supported."
                )
            self._merge_and_save(en_map, data_id)
        if self.cleanup:
            cleanup_dir(
                folder_path=self.download_folder,
                keep_extension=".zarr",
            )

    def _prepare_merge(
        self, files: list[str], data_id: str
    ) -> defaultdict[str, list[str]]:
        """Prepares files for merging by grouping them based on their Easting
        and Northing coordinates.

        Args:
            files: The list of files to be processed.
            data_id: The identifier for the dataset being processed.

        Returns:
            A dictionary mapping coordinates to lists of file paths.
        """
        en_map = defaultdict(list)
        data_id_folder = self.fs.sep.join([self.download_folder, data_id])
        for file in files:
            en = find_easting_northing(file)
            if en:
                en_map[en].append(self.fs.sep.join([data_id_folder, file]))
        return en_map

    def _merge_and_save(
        self, en_map: defaultdict[str, list[str]], data_id: str
    ) -> None:
        """Merges files along Easting and Northing axes and saves the final
        dataset using the data store.

        Args:
            en_map: A dictionary mapping coordinates to file lists.
            data_id: The identifier for the dataset being processed.
        """
        # Step 1: Group by Easting
        east_groups = defaultdict(list)
        for coord, file_list in en_map.items():
            easting = coord[:3]
            east_groups[easting].extend(file_list)

        # Step 2: Sort the Eastings and Northings. Reverse is true for the
        # values in the list because it is northings and they should be in
        # the descending order for the concat to happen correctly.
        sorted_east_groups = {
            key: sorted(value, reverse=True)
            for key, value in sorted(east_groups.items())
        }

        # Step 3: Merge files along the Y-axis (Northings) for each Easting
        # group. xarray takes care of the missing tiles and fills it with NaN
        # values
        merged_eastings = {}
        for easting, file_list in sorted_east_groups.items():
            datasets = []
            for file in file_list:
                with rasterio.open(file) as src:
                    chunk_size = self._get_chunk_size(
                        size_y=src.height, size_x=src.width
                    )
                da = rioxarray.open_rasterio(
                    file, masked=True, chunks=chunk_size, band_as_variable=True
                )
                datasets.append(da)
            merged_eastings[easting] = xr.concat(datasets, dim="y")

        final_datasets = list(merged_eastings.values())
        if not final_datasets:
            LOG.error("No files to merge!")
            return
        concat_cube = xr.concat(final_datasets, dim="x")

        final_cube = concat_cube.rename(
            dict(band_1=f"{data_id.split(DATA_ID_SEPARATOR)[-1]}")
        )
        new_filename = self.fs.sep.join([data_id + _ZARR_FORMAT])
        final_chunked_cube = chunk_dataset(
            final_cube,
            chunk_sizes={"x": self.tile_size[0], "y": self.tile_size[1]},
            format_name=_ZARR_FORMAT,
        )
        self.cache_store.write_data(final_chunked_cube, new_filename, replace=True)

    def _get_chunk_size(self, size_x: int, size_y: int) -> dict[str, int]:
        return {
            "x": ceil(size_x / ceil(size_x / self.tile_size[0])),
            "y": ceil(size_y / ceil(size_y / self.tile_size[1])),
        }


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
    current_time = datetime.now(tz=given_time.tzinfo)
    if (current_time - given_time) > timedelta(hours=TIME_TO_EXPIRE):
        return True
    else:
        return False
