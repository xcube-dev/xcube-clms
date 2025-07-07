from datetime import datetime
from typing import Any

import xarray as xr
from xcube.core.store import DataTypeLike
from xcube.util.jsonschema import JsonObjectSchema, JsonDateSchema

from xcube_clms.constants import (
    CLMS_API_URL,
    GET_DOWNLOAD_FILE_URLS_ENDPOINT,
    ACCEPT_HEADER,
    CONTENT_TYPE_HEADER,
)
from xcube_clms.product_handler import ProductHandler
from xcube_clms.utils import (
    get_response_of_type,
    make_api_request,
    build_api_url,
    get_authorization_header,
    _CHARACTERISTICS_TEMPORAL_EXTENT,
    get_extracted_component,
    _UID_KEY,
    _ID_KEY,
    is_valid_data_type,
    extract_and_filter_dates,
    detect_format,
)


class LegacyProductHandler(ProductHandler):
    """ """

    def __init__(
        self,
        cache_store=None,
        datasets_info=None,
        api_token_handler=None,
    ):
        super().__init__(cache_store, datasets_info, api_token_handler)
        # This handler does not need cache_store.

        self._api_token = self.api_token_handler.api_token

    @classmethod
    def product_type(cls):
        return "legacy"

    def has_data(self, data_id, data_type: DataTypeLike = None):
        if is_valid_data_type(data_type):
            dataset = get_extracted_component(
                datasets_info=self.datasets_info, data_id=data_id, item_type="product"
            )
            return bool(dataset)
        return False

    def get_open_data_params_schema(self, data_id: str = None) -> JsonObjectSchema:
        params = dict(time_range=JsonDateSchema.new_range())

        return JsonObjectSchema(
            properties=dict(**params),
            additional_properties=False,
        )

    def request_download(self, data_id: str) -> list[str]:
        self.api_token_handler.refresh_token()
        download_request_url, headers = self.prepare_request(data_id)
        response_data = make_api_request(
            method="GET", url=download_request_url, headers=headers
        )
        response = get_response_of_type(response_data, "json")
        return response

    def prepare_request(self, data_id: str) -> tuple[str, dict]:
        item = get_extracted_component(
            datasets_info=self.datasets_info, data_id=data_id, item_type="item"
        )
        product = get_extracted_component(
            datasets_info=self.datasets_info, data_id=data_id
        )

        dataset_uid = product[_UID_KEY]
        file_id = item[_ID_KEY]

        extra_params = {
            "dataset_uid": dataset_uid,
            "download_information_id": file_id,
        }
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
            CLMS_API_URL, GET_DOWNLOAD_FILE_URLS_ENDPOINT, extra_params=extra_params
        )

        headers = ACCEPT_HEADER.copy()
        headers.update(CONTENT_TYPE_HEADER)
        headers.update(get_authorization_header(self._api_token))

        return url, headers

    def preprocess_data(self, data_id, **preprocess_params):
        urls = self.request_download(data_id)
        time_range = preprocess_params.get("time_range")

        if not time_range:
            return urls

        filtered_urls = extract_and_filter_dates(urls, time_range)

        if len(filtered_urls) == 0:
            raise ValueError("No data found for the time range provided.")

        return filtered_urls

    def open_data(self, data_id: str, **open_params) -> Any:
        urls = self.preprocess_data(data_id, **open_params)
        fmt = detect_format(urls[0])
        if fmt == "netcdf":
            return xr.open_mfdataset(urls, engine="h5netcdf")
        elif fmt == "geotiff":
            return xr.open_mfdataset(urls, engine="rasterio")
        else:
            raise ValueError("Unsupported format detected.")
