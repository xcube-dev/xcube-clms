from typing import Any, Container, Iterator

from xcube.core.store import DataStoreError, DataTypeLike
from xcube.util.jsonschema import JsonDateSchema, JsonObjectSchema

from xcube_clms.constants import DATASET_DOWNLOAD_INFORMATION, ITEMS_KEY, NAME
from xcube_clms.product_handler import ProductHandler
from xcube_clms.utils import (
    detect_format,
    open_mfdataset_with_retry,
    generate_daily_ssm_paths,
)


class CdseProductHandler(ProductHandler):
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
        return "cdse"

    def get_open_data_params_schema(self, data_id: str = None) -> JsonObjectSchema:
        params = dict(time_range=JsonDateSchema.new_range())
        return JsonObjectSchema(
            properties=dict(**params),
            additional_properties=False,
        )

    def get_data_ids(
        self,
        data_type: DataTypeLike = None,
        include_attrs: Container[str] | bool = False,
        item: dict = None,
    ) -> Iterator[str | tuple[str, dict[str, Any]]]:
        dataset_download_info = item[DATASET_DOWNLOAD_INFORMATION][ITEMS_KEY][0]

        if dataset_download_info[NAME].lower() != "raster":
            return
        data_id = f"{item['id']}"

        if not include_attrs:
            yield data_id
        elif isinstance(include_attrs, bool) and include_attrs:
            yield data_id, dataset_download_info
        elif isinstance(include_attrs, list):
            filtered_attrs = {
                attr: dataset_download_info[attr]
                for attr in include_attrs
                if attr in dataset_download_info
            }
            yield data_id, filtered_attrs

    def request_download(self, data_id: str) -> list[str]:
        pass

    def prepare_request(self, data_id: str) -> tuple[str, dict]:
        pass

    def open_data(self, data_id: str, **open_params) -> Any:
        time_range = open_params.get("time_range")
        if time_range is not None:
            urls = generate_daily_ssm_paths(time_range[0], time_range[1])
        else:
            urls = []
            #     urls = generate_daily_ssm_paths(
            #         SOIL_MOISTURE_START_DATE, SOIL_MOISTURE_END_DATE
            #     )

        if len(urls) == 0:
            raise DataStoreError("No data found for the time range provided.")
        print("urls::", urls)
        fmt = detect_format(urls[0])
        if fmt == "netcdf":
            try:
                return open_mfdataset_with_retry(urls, engine="h5netcdf")
            except Exception:
                return open_mfdataset_with_retry(urls)
        elif fmt == "geotiff":
            return open_mfdataset_with_retry(urls, engine="rasterio")
        else:
            raise DataStoreError("Unsupported format detected.")
