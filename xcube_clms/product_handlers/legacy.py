from datetime import datetime
from typing import Any

from xcube.core.store import DataStoreError, DataTypeLike
from xcube.util.jsonschema import JsonDateSchema, JsonObjectSchema

from xcube_clms.constants import (ACCEPT_HEADER, CLMS_API_URL,
                                  CONTENT_TYPE_HEADER,
                                  GET_DOWNLOAD_FILE_URLS_ENDPOINT, ID_KEY,
                                  UID_KEY)
from xcube_clms.product_handler import ProductHandler
from xcube_clms.utils import (build_api_url, detect_format,
                              extract_and_filter_dates,
                              get_authorization_header,
                              get_extracted_component, get_response_of_type,
                              is_valid_data_type, make_api_request,
                              open_mfdataset_with_retry)

_CHARACTERISTICS_TEMPORAL_EXTENT = "characteristics_temporal_extent"


class LegacyProductHandler(ProductHandler):
    """ """

    def __init__(
        self,
        datasets_info=None,
        cache_store=None,
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

        dataset_uid = product[UID_KEY]
        file_id = item[ID_KEY]

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

    def filter_urls(self, data_id, **preprocess_params):
        """
        Filters download URLs for a given data ID based on an optional time range.

        This method requests the available download URLs for a data_id and,
        if a `time_range` is provided in `preprocess_params`, filters the
        URLs to include only those that fall within the specified time range.

        Args:
            data_id: The identifier of the dataset.
            **preprocess_params: Optional parameters, including `time_range` as
                a tuple (start_date, end_date).

        Returns:
            A list of filtered download URLs.

        Raises:
            DataStoreError: If no URLs match the provided time range.
        """
        urls = self.request_download(data_id)
        time_range = preprocess_params.get("time_range")

        if not time_range:
            return urls

        filtered_urls = extract_and_filter_dates(urls, time_range)

        if len(filtered_urls) == 0:
            raise DataStoreError("No data found for the time range provided.")

        return filtered_urls

    def open_data(self, data_id: str, **open_params) -> Any:
        urls = self.filter_urls(data_id, **open_params)
        fmt = detect_format(urls[0])
        if fmt == "netcdf":
            return open_mfdataset_with_retry(urls, engine="h5netcdf")
        elif fmt == "geotiff":
            return open_mfdataset_with_retry(urls, engine="rasterio")
        else:
            raise DataStoreError("Unsupported format detected.")
