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

from typing import Any, Callable

from xcube.core.store import PreloadedDataStore
from xcube.core.store.preload import ExecutorPreloadHandle, PreloadHandle, PreloadState

from .constants import DOWNLOAD_FOLDER
from .utils import cleanup_dir


class ClmsPreloadHandle(ExecutorPreloadHandle):
    """Handles the preloading of data into the cache store.

    Authentication credentials can be obtained following the steps from the
    CLMS API documentation
    https://eea.github.io/clms-api-docs/authentication.html
    """

    def __init__(
        self,
        data_id_maps: dict[str, dict[str, dict[str, Any]]],
        cache_store: PreloadedDataStore,
        preload_data: Callable[[PreloadHandle, str], None],
        **preload_params,
    ) -> None:
        self.data_id_maps = data_id_maps
        self.fs = cache_store.fs
        self._download_folder = self.fs.sep.join([cache_store.root, DOWNLOAD_FOLDER])
        super().__init__(
            data_ids=tuple(self.data_id_maps.keys()),
            blocking=preload_params.pop("blocking", True),
            silent=preload_params.pop("silent", False),
            preload_data=preload_data,
        )

    def close(self) -> None:
        for data_id in self.data_id_maps.keys():
            self.notify(
                PreloadState(data_id=data_id, message="Cleaning up in Progress...")
            )
        cleanup_dir(self._download_folder)
        for data_id in self.data_id_maps.keys():
            self.notify(PreloadState(data_id=data_id, message="Cleaning up Finished."))
