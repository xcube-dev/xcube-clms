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

from xcube.core.store import new_data_store, DataStore

from xcube_clms.constants import LOG, DATA_ID_SEPARATOR


class CacheManager:
    """Manage the cache map for preloaded data.
    """

    def __init__(self, path) -> None:
        """
        Initialize the CacheManager with the specified path.

        Args:
            path: The local path where the cache and file store are managed.
        """
        self.cache = None
        self.path = path
        os.makedirs(self.path, exist_ok=True)
        LOG.info(f"Local Filestore for preload cache created at {self.path}")
        self.file_store = new_data_store("file", root=self.path)
        self.refresh_cache()

    def refresh_cache(self) -> None:
        """
        Refreshes the cache by updating the names of files and data_ids on the
        cache path. Since the name of the folder is the actual data_id the
        mapping here is data_id: file_name.

        For e.g. if data_id = forest-type-2018|FTY-2018 and a folder exists
        with the same name, the file inside will be FTY-2018.tif or
        FTY-2018.zarr depending on whether this download contained several
        files (converted to .zarr) or a single file (.tif left as is).

        So, the cache map would look something like this
        {`forest-type-2018|FTY-2018`: `FTY-2018.tif`}

        We use this for lookup in case the user requests to preload or open the
        data that exists in the cache map already.
        """
        self.cache = {
            d: os.path.join(self.path, d)
            for d in os.listdir(self.path)
            if DATA_ID_SEPARATOR in d
        }

    def get_file_store(self) -> DataStore:
        """
        Retrieve the local file store.

        Returns:
            The file store object used for managing files in the cache like
            opening or writing.
        """
        return self.file_store

    def get_cache(self) -> dict[str, str]:
        """
        Retrieve the current cache map.

        Returns:
            A dictionary representing the cache, with data IDs as keys and
            their file paths as values.
        """
        return self.cache
