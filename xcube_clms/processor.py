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

import re
from collections import defaultdict
from math import ceil
from pathlib import Path
from typing import Optional, Final

import fsspec
import rasterio
import rioxarray
import xarray as xr
from xcube.core.chunk import chunk_dataset

from xcube_clms.constants import DATA_ID_SEPARATOR, DOWNLOAD_FOLDER
from xcube_clms.constants import LOG

_ZARR_FORMAT = ".zarr"
_PREFERRED_CHUNK_SIZE: Final = 2000


class FileProcessor:
    """Handles file processing after download completes."""

    def __init__(
        self,
        cache_store,
        cleanup: bool = True,
        tile_size: int | tuple[int, int] = None,
    ) -> None:
        self.cache_store = cache_store
        self.fs = cache_store.fs
        self.cleanup = cleanup

        if tile_size is None:
            self.tile_size = (_PREFERRED_CHUNK_SIZE, _PREFERRED_CHUNK_SIZE)
        elif isinstance(tile_size, int):
            self.tile_size = (tile_size, tile_size)
        else:
            self.tile_size = tile_size

        self.download_folder = self.fs.sep.join(
            [self.cache_store.root, DOWNLOAD_FOLDER]
        )
        self.target_folder = None

    def preprocess(self, data_id: str) -> None:
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

        self.target_folder = self.fs.sep.join([self.download_folder, data_id])
        files = [entry.split("/")[-1] for entry in self.fs.ls(self.target_folder)]
        if len(files) == 1:
            LOG.debug("Converting the file to zarr format.")
            cache_data_id = self.fs.sep.join([DOWNLOAD_FOLDER, data_id, files[0]])
            data = self.cache_store.open_data(cache_data_id)
            new_cache_data_id = data_id + _ZARR_FORMAT
            data = chunk_dataset(
                data,
                chunk_sizes={"x": self.tile_size[0], "y": self.tile_size[1]},
                format_name=_ZARR_FORMAT,
            )
            final_cube = data.rename(
                dict(band_1=f"{data_id.split(DATA_ID_SEPARATOR)[-1]}")
            )
            final_cube_path = self.fs.sep.join(
                [self.cache_store.root, new_cache_data_id]
            )
            if self.fs.isdir(final_cube_path):
                self.fs.rm(final_cube_path, recursive=True)
            self.cache_store.write_data(final_cube, new_cache_data_id)
        elif len(files) == 0:
            LOG.warn("No files to preprocess!")
        else:
            en_map = self._prepare_merge(files)
            if not en_map:
                LOG.error(
                    "This naming format is not supported. Currently "
                    "only filenames with Eastings and Northings are "
                    "supported."
                )
                return
            self._merge_and_save(en_map, data_id)
        if self.cleanup:
            cleanup_dir(
                folder_path=self.download_folder,
                keep_extension=".zarr",
            )

    def _prepare_merge(self, files: list[str]) -> defaultdict[str, list[str]]:
        """Prepares files for merging by grouping them based on their Easting
        and Northing coordinates.

        Args:
            files: The list of files to be processed.

        Returns:
            A dictionary mapping coordinates to lists of file paths.
        """
        en_map = defaultdict(list)
        for file in files:
            en = find_easting_northing(file)
            if en:
                en_map[en].append(self.fs.sep.join([self.target_folder, file]))
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
        final_cube_path = self.fs.sep.join([self.cache_store.root, new_filename])
        if self.fs.isdir(final_cube_path):
            self.fs.rm(final_cube_path, recursive=True)
        self.cache_store.write_data(final_chunked_cube, new_filename)

    def _get_chunk_size(self, size_x: int, size_y: int) -> dict[str, int]:
        return {
            "x": ceil(size_x / ceil(size_x / self.tile_size[0])),
            "y": ceil(size_y / ceil(size_y / self.tile_size[1])),
        }


def find_easting_northing(name: str) -> Optional[str]:
    """Finds the easting/northing coordinate pattern in the provided filename.

    This function searches for a specific pattern, "E##N##", in a string
    and returns the first match if found.

    Args:
        name: The string to search for the easting/northing pattern.

    Returns:
        The matched coordinate string if found, otherwise None.
    """
    match = re.search(r"[E]\d{2}[N]\d{2}", name)
    if match:
        return match.group(0)
    return None


def cleanup_dir(folder_path: Path | str, fs=None, keep_extension=None):
    """Removes all files from a directory, retaining only those with the
    specified extension in the root directory.

    Args:
        folder_path: The path to the directory to clean up.
        fs: A fsspec filesystem object. If None, the local filesystem is used.
            Optional.
        keep_extension: The file extension to retain. Optional
    """
    folder_path = str(folder_path)
    fs = fs or fsspec.filesystem("file")

    if not fs.isdir(folder_path):
        raise ValueError(f"The specified path {folder_path} is not a directory.")

    for item in fs.listdir(folder_path):
        item_path = item["name"]
        try:
            # Adding the `not item_path.endswith(keep_extension)` condition
            # here as `.zarr` files are recognized as folders
            if fs.isdir(item_path) and (
                keep_extension is None
                or (keep_extension and not item_path.endswith(keep_extension))
            ):
                fs.rm(item_path, recursive=True)
                LOG.debug(f"Deleted directory: {item_path}")
            else:
                if keep_extension and item_path.endswith(keep_extension):
                    LOG.debug(f"Kept file: {item_path}")
                else:
                    fs.rm(item_path)
                    LOG.debug(f"Deleted file: {item_path}")
        except Exception as e:
            LOG.error(f"Failed to delete {item_path}: {e}")
    LOG.debug(f"Cleaning up finished")
