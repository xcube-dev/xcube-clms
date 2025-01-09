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
import threading
import time
from typing import Any

import fsspec
from xcube.core.store import MutableDataStore
from xcube.core.store.preload import ExecutorPreloadHandle, PreloadState, \
    PreloadStatus

from xcube_clms.api_token_handler import ClmsApiTokenHandler
from xcube_clms.constants import (
    COMPLETE,
    RETRY_TIMEOUT,
    CANCELLED,
)
from xcube_clms.download_manager import DownloadTaskManager
from xcube_clms.processor import FileProcessor, cleanup_dir


class ClmsPreloadHandle(ExecutorPreloadHandle):
    """Handles the preloading of data into the cache store.

    Authentication credentials can be obtained following the steps from the
    CLMS API documentation
    https://eea.github.io/clms-api-docs/authentication.html
    """

    def __init__(
        self,
        data_id_maps: dict[str, dict[str, dict[str, Any]]],
        url: str,
        credentials: dict,
        cache_store: MutableDataStore,
        **preload_params,
    ) -> None:
        self.data_id_maps = data_id_maps
        self._url: str = url
        self.cache_store = cache_store
        self.cache_root = cache_store.root
        self._cache_fs: fsspec.AbstractFileSystem = cache_store.fs

        self._token_handler = ClmsApiTokenHandler(credentials)
        self.cleanup = preload_params.pop("cleanup", True)
        self.disable_tqdm_progress = preload_params.pop("disable_tqdm_progress", True)
        self._file_processor = FileProcessor(
            self.cache_store,
            self.cleanup,
            self.disable_tqdm_progress,
        )
        self._download_manager = DownloadTaskManager(
            self._token_handler, self._url, self.cache_store, self.disable_tqdm_progress
        )

        super().__init__(
            data_ids=tuple(self.data_id_maps.keys()),
            blocking=preload_params.pop("blocking", True),
            silent=preload_params.pop("silent", False),
        )

    def preload_data(
        self,
        data_id: str,
    ) -> None:
        """Processes a single preload task on a separate thread."""
        status_event = threading.Event()
        data_id_info = self.data_id_maps.get(data_id)
        if data_id in (
            element.split("/")[0] for element in self.cache_store.list_data_ids()
        ):
            self.notify(
                PreloadState(
                    data_id,
                    status=PreloadStatus.stopped,
                    progress=1.0,
                    message=f"The data for {data_id} is already cached at {self.cache_root}",
                )
            )
            return

        task_id = self._download_manager.request_download(
            data_id=data_id,
            item=data_id_info.get("item"),
            product=data_id_info.get("product"),
        )

        self.notify(
            PreloadState(
                data_id=data_id,
                progress=0.1,
                message="Download request in queue.",
            )
        )
        status_event.set()

        while status_event.is_set():
            status, _ = self._download_manager.get_current_requests_status(
                task_id=task_id
            )
            if status == COMPLETE:
                status_event.clear()
                self.notify(
                    PreloadState(
                        data_id=data_id,
                        progress=0.4,
                        message="Download link created. Downloading now...",
                    )
                )
                download_url, _ = self._download_manager.get_download_url(task_id)
                self.download_data(download_url, data_id)
                self.notify(
                    PreloadState(
                        data_id=data_id,
                        progress=0.8,
                        message="Zip file downloaded. Extracting now...",
                    )
                )
                self._file_processor.preprocess(data_id)
                self.notify(
                    PreloadState(
                        data_id=data_id,
                        progress=1.0,
                        message="Preloading Complete.",
                    )
                )
                return
            if status in CANCELLED:
                status_event.clear()
                self.cancel()
                self.close()
                return

            time.sleep(RETRY_TIMEOUT)

    def close(self) -> None:
        for data_id in self.data_id_maps.keys():
            self.notify(
                PreloadState(data_id=data_id, message="Cleaning up in Progress...")
            )
        if self.cleanup:
            cleanup_dir(self.cache_root)
        for data_id in self.data_id_maps.keys():
            self.notify(PreloadState(data_id=data_id, message="Cleaning up Finished."))

    def download_data(self, download_url: str, data_id: str) -> None:
        """Initiates the download process for a given data item.

        Args:
            download_url: URL of the data to download. This is obtained from
                the completed request of a dataset.
            data_id: Identifier of the data being downloaded.
        """
        self._download_manager.download_data(download_url, data_id)
