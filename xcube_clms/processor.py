# The MIT License (MIT)
# Copyright (c) 2024 by the xcube development team and contributors
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
from collections import defaultdict

import rioxarray
import xarray as xr
from tqdm.notebook import tqdm

from xcube_clms.constants import LOG, DATA_ID_SEPARATOR
from xcube_clms.utils import find_easting_northing, cleanup_dir


class FileProcessor:
    def __init__(self, path, file_store, cleanup=True):
        self.path = path
        self.file_store = file_store
        self.cleanup = cleanup

    def postprocess(self, data_id: str):
        target_folder = os.path.join(self.path, data_id)
        files = os.listdir(target_folder)
        if len(files) == 1:
            LOG.info("No postprocessing required.")
        elif len(files) == 0:
            LOG.warn("No files to postprocess!")
        else:
            en_map = self._prepare_merge(files, data_id)
            if en_map is {}:
                LOG.error(
                    "This naming format is not supported. Currently "
                    "only filenames with Eastings and Northings are "
                    "supported."
                )
                return
            self._merge_and_save(en_map, data_id)
            if self.cleanup:
                cleanup_dir(target_folder)

    def _prepare_merge(self, files: list[str], data_id: str):
        en_map = defaultdict(list)
        data_id_folder = os.path.join(self.path, data_id)
        for file in files:
            en = find_easting_northing(file)
            en_map[en].append(os.path.join(data_id_folder, file))
        return en_map

    def _merge_and_save(self, en_map: defaultdict[str, list], data_id: str):
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
        chunk_size = {"x": 1000, "y": 1000}
        merged_eastings = {}
        for easting, file_list in tqdm(
            sorted_east_groups.items(),
            desc=f"Concatenating along the Y-axis (Northings)",
        ):
            datasets = []
            for file in file_list:
                da = rioxarray.open_rasterio(file, masked=True, chunks=chunk_size)
                datasets.append(da)
            merged_eastings[easting] = xr.concat(datasets, dim="y")

        # Step 4: Merge along the X-axis (Easting)
        final_datasets = list(merged_eastings.values())
        final_cube = xr.concat(final_datasets, dim="x")

        final_cube = final_cube.to_dataset(
            name=f"{data_id.split(DATA_ID_SEPARATOR)[-1]}"
        )

        new_filename = os.path.join(
            data_id, data_id.split(DATA_ID_SEPARATOR)[-1] + ".zarr"
        )
        # self._update_cache(data_id, new_filename)
        self.file_store.write_data(final_cube, new_filename)
