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

from xcube.core.store import MutableDataStore
from xcube.core.store.preload import ExecutorPreloadHandle, PreloadState, \
    PreloadStatus

from xcube_clms.api_token_handler import ClmsApiTokenHandler
from xcube_clms.cache_manager import CacheManager
from xcube_clms.constants import (
    COMPLETE,
    RETRY_TIMEOUT,
    CANCELLED,
)
from xcube_clms.download_manager import DownloadTaskManager
from xcube_clms.processor import FileProcessor, cleanup_dir


class ClmsPreloadHandle(ExecutorPreloadHandle):
    """Handles the preloading of data into the cache store."""

    def __init__(
        self,
        data_id_maps: dict[str, dict[str, dict[str, Any]]],
        url: str,
        credentials: dict,
        path: str,
        blocking: bool = None,
        silent: bool | None = None,
        cleanup: bool | None = None,
        disable_tqdm_progress: bool | None = None,
        data_store: str | None = None,
    ) -> None:
        """Initializes the PreloadData instance.

        Args:
            url: The base URL for data requests to the CLMS API.
            credentials: Authentication credentials that are obtained
                following the steps from the CLMS API documentation.
                https://eea.github.io/clms-api-docs/authentication.html
            path: Local path for caching and file storage.
            cleanup: Whether to clean up the extracted files from the zip
            download. Defaults to True.
        """
        self.data_id_maps = data_id_maps
        self._url: str = url
        self._credentials: dict = {}
        self.path: str = path

        self._task_control: dict = {}
        self.cleanup: bool = cleanup or True

        self._token_handler = ClmsApiTokenHandler(credentials)
        self._api_token: str = self._token_handler.api_token
        self._cache_manager = CacheManager(self.path, data_store)
        self._cache_manager.refresh_cache()
        self.data_store: MutableDataStore = self._cache_manager.data_store
        self._file_processor = FileProcessor(
            self.path, self.data_store, self.cleanup, disable_tqdm_progress
        )
        self._download_manager = DownloadTaskManager(
            self._token_handler, self._url, self.path, disable_tqdm_progress
        )
        super().__init__(
            data_ids=tuple(self.data_id_maps.keys()), blocking=blocking, silent=silent
        )

    def preload_data(
        self,
        data_id: str,
    ) -> None:
        """Processes a single preload task on a separate thread."""
        status_event = threading.Event()
        self.refresh_cache()
        data_id_info = self.data_id_maps.get(data_id)
        if data_id in self.view_cache().keys():
            self.notify(
                PreloadState(
                    data_id,
                    status=PreloadStatus.stopped,
                    progress=1.0,
                    message=f"The data for {data_id} is already cached at {self.path}",
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
                self._file_processor.postprocess(data_id)
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
        cleanup_dir(self.path)
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

    def view_cache(self) -> dict[str, str]:
        """Retrieves the current cache map.

        Returns:
            dict: Cached map.
        """
        return self._cache_manager.cache

    def refresh_cache(self) -> None:
        """Refreshes the cache map."""
        self._cache_manager.refresh_cache()
