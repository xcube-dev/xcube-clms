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

import threading
import time
from typing import Any

import fsspec
from xcube.core.store import PreloadedDataStore
from xcube.core.store.preload import ExecutorPreloadHandle
from xcube.core.store.preload import PreloadState
from zappend.api import zappend

from xcube_clms.api_token_handler import ClmsApiTokenHandler
from xcube_clms.constants import CANCELLED, DOWNLOAD_FOLDER, DATA_ID_SEPARATOR
from xcube_clms.constants import COMPLETE
from xcube_clms.constants import RETRY_TIMEOUT
from xcube_clms.download_manager import DownloadTaskManager
from xcube_clms.processor import FileProcessor
from xcube_clms.processor import cleanup_dir


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
        cache_store: PreloadedDataStore,
        **preload_params,
    ) -> None:
        self.data_id_maps = data_id_maps
        self._url: str = url
        self._cache_store = cache_store
        self._cache_root = cache_store.root
        self._cache_fs: fsspec.AbstractFileSystem = cache_store.fs

        self._token_handler = ClmsApiTokenHandler(credentials=credentials)
        self.cleanup = preload_params.pop("cleanup", True)
        self.tile_size = preload_params.pop("tile_size", None)
        self._file_processor = FileProcessor(
            cache_store=self._cache_store,
            cleanup=self.cleanup,
            tile_size=self.tile_size,
        )
        self._download_manager = DownloadTaskManager(
            token_handler=self._token_handler,
            url=self._url,
            cache_store=self._cache_store,
        )

        self._download_folder = self._cache_fs.sep.join(
            [self._cache_root, DOWNLOAD_FOLDER]
        )

        if self._cache_fs.isdir(self._download_folder) and self.cleanup:
            cleanup_dir(self._download_folder)

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

        # EEA datasets
        if DATA_ID_SEPARATOR in data_id:
            task_id = self._download_manager.request_download(
                data_id=data_id,
                item=data_id_info.get("item"),
                product=data_id_info.get("product"),
            )

            self.notify(
                PreloadState(
                    data_id=data_id,
                    progress=0.1,
                    message=f"Task ID {task_id}: Download request in queue.",
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
                            message=f"Task ID {task_id}: Download link created. "
                            f"Downloading and extracting now...",
                        )
                    )
                    download_url, _ = self._download_manager.get_download_url(task_id)
                    self._download_manager.download_zip_data(download_url, data_id)
                    self.notify(
                        PreloadState(
                            data_id=data_id,
                            progress=0.8,
                            message=f"Task ID {task_id}: Extraction complete. "
                            f"Processing now...",
                        )
                    )
                    self._file_processor.preprocess(data_id)
                    self.notify(
                        PreloadState(
                            data_id=data_id,
                            progress=1.0,
                            message=f"Task ID {task_id}: Preloading Complete.",
                        )
                    )
                    return
                if status in CANCELLED:
                    status_event.clear()
                    self.notify(
                        PreloadState(
                            data_id=data_id,
                            message=f"Task ID {task_id}: Download request was cancelled by the user from "
                            "the Land Copernicus UI.",
                        )
                    )
                    self.cancel()
                    return

                time.sleep(RETRY_TIMEOUT)

        else:
            self.notify(
                PreloadState(
                    data_id=data_id,
                    progress=0.1,
                    message=f"Requesting URLs for download",
                )
            )
            links = self._download_manager.request_download(
                data_id=data_id,
                item=data_id_info.get("item"),
                product=data_id_info.get("product"),
            )
            self.notify(
                PreloadState(
                    data_id=data_id,
                    progress=0.2,
                    message=f"Downloaded URLs. Starting download now.",
                )
            )

            # Max calls allowed per second is 10, but based on tests,
            # that fails sometimes, and we use a conservative value of 8
            MAX_CALLS_PER_SECOND = 8
            delay = 1 / MAX_CALLS_PER_SECOND

            total_files = len(links)

            progress_start = 20
            progress_end = 60
            progress_range = progress_end - progress_start

            for i, url in enumerate(links):
                self._download_manager.download_file(url, data_id)
                progress = progress_start + ((i + 1) / total_files) * progress_range
                if i % MAX_CALLS_PER_SECOND == 0:
                    self.notify(
                        PreloadState(
                            data_id=data_id,
                            progress=progress / 100,
                            message=f"Downloading ...",
                        )
                    )

                # To avoid getting too many HTTP requests error from the server
                if i % 200 == 0:
                    time.sleep(3)
                time.sleep(delay)

            self.notify(
                PreloadState(
                    data_id=data_id,
                    progress=0.6,
                    message=f"Downloading files complete. Creating .zarr...",
                )
            )

            items = self._cache_fs.listdir(
                self._cache_fs.sep.join([self._download_folder, data_id])
            )
            files = sorted(
                [item["name"] for item in items if (item.get("type") != "directory")]
            )
            target_path = self._cache_fs.sep.join([self._cache_root, data_id + ".zarr"])

            if self._cache_fs.exists(target_path):
                self._cache_fs.rm(target_path, recursive=True)

            zappend(files, target_dir=target_path, tile_size=self.tile_size)

            self.notify(
                PreloadState(
                    data_id=data_id,
                    progress=1.0,
                    message=f"Preloading complete",
                )
            )

    def close(self) -> None:
        for data_id in self.data_id_maps.keys():
            self.notify(
                PreloadState(data_id=data_id, message="Cleaning up in Progress...")
            )
        cleanup_dir(self._download_folder)
        for data_id in self.data_id_maps.keys():
            self.notify(PreloadState(data_id=data_id, message="Cleaning up Finished."))
