import io

import fsspec
import rioxarray
import xarray as xr

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
)
from xcube_clms.utils import (
    make_api_request,
    get_response_of_type,
    get_authorization_header,
    get_dataset_download_info,
    build_api_url,
)


class PreloadData:
    def __init__(self, url, credentials):
        self._credentials: dict = {}
        self._api_token = None
        self._clms_api_token_instance = None
        self._url = url
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

    def queue_download(
        self, data_id: str, item: dict, product: dict
    ) -> tuple[str, str]:
        self._refresh_token()

        # This is to make sure that there are pre-packaged files available for download.
        # Without this, the API throws the following error: Error, the FileID is not valid.
        # We check for path and source based on the API code here:
        # https://github.com/eea/clms.downloadtool/blob/master/clms/downloadtool/api/services/datarequest_post/post.py#L177-L196

        path = item.get(PATH_KEY, "")
        source = item.get(SOURCE_KEY, "")

        if (path is not "") and (source is not ""):
            LOG.warning(f"No prepackaged downloadable items available for {data_id}")

        full_source = (
            item.get(DATASET_DOWNLOAD_INFORMATION_KEY)
            .get(ITEMS_KEY)[0]
            .get(FULL_SOURCE_KEY, "")
        )

        assert (
            full_source in NOT_SUPPORTED_LIST
        ), f"This data product: {item[TITLE_KEY]} is not yet supported in this plugin yet."

        status, task_id = self._get_current_requests_status(product[UID_KEY])
        if status == COMPLETE:
            LOG.info(
                f"Download request with task id {task_id} already completed for data id: {data_id}"
            )
            return task_id, self.download_url
        if status == UNDEFINED:
            LOG.info(f"No download request exists for data id: {data_id}")

        download_request_url, headers, json = self._prepare_download_request(
            data_id, item, product
        )

        print(download_request_url, headers, json)

        # response_data = make_api_request(
        #     method="POST", url=download_request_url, headers=headers, json=json
        # )
        # response = get_response_of_type(response_data, JSON_TYPE)
        # task_ids = response.get(TASK_IDS_KEY)
        # assert (
        #     len(task_ids) == 1
        # ), f"Expected API response with 1 task_id, got {len(task_ids)}"
        # task_id = task_ids[0].get(TASK_ID_KEY)
        # LOG.info(f"Download Requested with Task ID : {task_id}")
        # return task_id

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

    def _get_current_requests_status(self, dataset_id: str) -> tuple[str, str]:
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

            if status in STATUS_PENDING and dataset_id == requested_data_id:
                return PENDING, key
            if status in STATUS_COMPLETE and dataset_id == requested_data_id:
                self.download_url = response[key][0][DOWNLOAD_URL_KEY]
                return COMPLETE, key

        return UNDEFINED, ""

    def request_download(self):
        LOG.info(f"Downloading zip file from {self.download_url}")
        headers = ACCEPT_HEADER.copy()
        headers.update(CONTENT_TYPE_HEADER)
        response_data = make_api_request(
            self.download_url, headers=headers, stream=True
        )
        response = get_response_of_type(response_data, BYTES_TYPE)

        with io.BytesIO(response.content) as zip_file_in_memory:
            fs = fsspec.filesystem("zip", fo=zip_file_in_memory)
            zip_contents = fs.ls(RESULTS)
            actual_zip_file = None
            if len(zip_contents) == 1:
                if ".zip" in zip_contents[0][FILENAME_KEY]:
                    actual_zip_file = zip_contents[0]
            elif len(zip_contents) > 1:
                LOG.warn("Cannot handle more than one zip files at the moment.")
            else:
                LOG.info("No downloadable zip file found inside.")
            if actual_zip_file:
                LOG.info(f"Found one zip file {actual_zip_file}.")
                with fs.open(actual_zip_file[NAME_KEY], "rb") as f:
                    zip_fs = fsspec.filesystem("zip", fo=f)
                    geo_file = self._find_geo_in_dir(
                        "/",
                        zip_fs,
                    )
                    if geo_file:
                        with zip_fs.open(geo_file, "rb") as geo_f:
                            if "tif" in geo_file:
                                return rioxarray.open_rasterio(geo_f)
                            elif "nc" in geo_file:
                                return xr.open_dataset(geo_f)
                    else:
                        raise Exception("No file found in the download to load")

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
