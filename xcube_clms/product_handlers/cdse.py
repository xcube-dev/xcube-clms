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


import os
from datetime import datetime
from typing import Any, Container, Iterator

import pandas as pd
import rasterio
import rioxarray
import xarray as xr
from dotenv import load_dotenv
from xcube.core.store import (DataDescriptor, DatasetDescriptor, DataStore,
                              DataStoreError, DataTypeLike)
from xcube.util.jsonschema import JsonDateSchema, JsonObjectSchema

from xcube_clms.api_token_handler import ClmsApiTokenHandler
from xcube_clms.constants import (CRS_KEY, DATASET_DOWNLOAD_INFORMATION,
                                  ITEMS_KEY, LOG, NAME)
from xcube_clms.product_handler import ProductHandler
from xcube_clms.utils import detect_format, normalize_time_range, to_bbox

load_dotenv()

supported_clms_products_cdse = {
    "daily-surface-soil-moisture-v1.0": "https://s3.waw3-1.cloudferro.com/swift/v1/CatalogueCSV/bio"
    "-geophysical/surface_soil_moisture/ssm_europe_1km_daily_v1"
    "/ssm_europe_1km_daily_v1_nc.csv"
}


class CdseProductHandler(ProductHandler):
    """Product handler to open CLMS products lazily from CDSE S3 endpoint"""

    def __init__(
        self,
        cache_store: DataStore = None,
        datasets_info: list[dict] = None,
        api_token_handler: ClmsApiTokenHandler = None,
    ):
        super().__init__(cache_store, datasets_info, api_token_handler)
        # This handler does not need cache_store.

        self._api_token = self.api_token_handler.api_token

        session = rasterio.session.AWSSession(
            aws_unsigned=False,
            endpoint_url="eodata.dataspace.copernicus.eu",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
        env = rasterio.env.Env(session=session, AWS_VIRTUAL_HOSTING=False)
        # keep the rasterio environment open so that the data can be accessed
        # when plotting or writing the data
        env = env.__enter__()

    @classmethod
    def product_type(cls):
        return "cdse"

    def get_open_data_params_schema(self, data_id: str = None) -> JsonObjectSchema:
        params = dict(time_range=JsonDateSchema.new_range())
        return JsonObjectSchema(
            properties=dict(**params),
            additional_properties=False,
        )

    def get_data_id(
        self,
        data_type: DataTypeLike = None,
        include_attrs: Container[str] | bool = False,
        item: dict = None,
    ) -> Iterator[str | tuple[str, dict[str, Any]]]:
        dataset_download_info = item[DATASET_DOWNLOAD_INFORMATION][ITEMS_KEY][0]

        if dataset_download_info[NAME].lower() != "raster":
            return
        data_id = f"{item['id']}"

        if data_id not in supported_clms_products_cdse.keys():
            return

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

    def describe_data(self, data_id: str, product: dict) -> DataDescriptor:
        df = _get_df_from_data_id(data_id)
        min_date, max_date = _get_min_max_date(df)

        bbox = None

        if "bbox" in df.columns:
            bboxes = df["bbox"]

            if len(bboxes) > 1 and (bboxes[0] != bboxes[1]):
                raise ValueError(
                    f"Different bboxes {bboxes[0]}, {bboxes[1]} "
                    f"found for this product {data_id}"
                )

            raw_bbox = bboxes[0]  # picking the first one as it is assumed that
            # all the datasets in this csv have the same spatial dimensions and
            # only differ temporally.

            bbox = to_bbox(raw_bbox)

        crs = product.get(CRS_KEY, [])
        normalized_time_range = normalize_time_range((min_date, max_date))

        metadata = dict(time_range=normalized_time_range, crs=crs[0] if crs else None)

        if bbox is not None:
            metadata["bbox"] = bbox

        return DatasetDescriptor(data_id, **metadata)

    def request_download(self, data_id: str) -> list[str]:
        # Not required for this store.
        pass

    def prepare_request(self, data_id: str) -> tuple[str, dict]:
        # Not required for this store.
        pass

    def open_data(self, data_id: str, **open_params) -> Any:
        time_range = open_params.get("time_range")
        urls = []
        if time_range is not None:
            urls = _generate_daily_ssm_paths(data_id, time_range[0], time_range[1])

        if len(urls) == 0:
            raise DataStoreError("No data found for the time range provided.")
        fmt = detect_format(urls[0])
        if fmt == "netcdf":
            final_ds_list = []

            for i, path in enumerate(urls):
                ds = rioxarray.open_rasterio(
                    path,
                    chunks=dict(y=1000, x=1000),
                    driver="netCDF",
                )
                final_ds_list.append(ds)

            final_ds = xr.concat(final_ds_list, dim="time")
            return final_ds
        else:
            raise DataStoreError("Unsupported format detected.")


def _generate_daily_ssm_paths(data_id: str, start_date: str, end_date: str) -> list:
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    df = _get_df_from_data_id(data_id)
    min_date, max_date = _get_min_max_date(df)

    if min_date > start_date:
        LOG.warn(f"No data available before {min_date}.")
    if max_date < end_date:
        LOG.warn(f"No data available after {max_date}.")

    mask = (df["content_date_start"] >= start_date) & (
        df["content_date_start"] <= end_date
    )
    paths = df.loc[mask, "s3_path"].tolist()
    fixed_paths = [_append_nc_file(p) for p in paths]
    return fixed_paths


def _get_df_from_data_id(data_id: str) -> pd.DataFrame:
    url = supported_clms_products_cdse[data_id]
    return pd.read_csv(url, sep=";")


def _get_min_max_date(df: pd.DataFrame) -> tuple[datetime, datetime]:
    df["content_date_start"] = pd.to_datetime(df["content_date_start"])
    min_date = df["content_date_start"].min()
    max_date = df["content_date_start"].max()

    return min_date, max_date


def _append_nc_file(path: str):
    filename_nc = os.path.basename(path)
    filename_dot_nc = filename_nc.replace("_nc", ".nc")
    return path + "/" + filename_dot_nc
