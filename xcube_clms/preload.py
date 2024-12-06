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
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from xcube_clms.cache_manager import CacheManager
from xcube_clms.constants import (
    LOG,
    COMPLETE,
    PENDING,
    RETRY_TIMEOUT,
    CANCELLED,
)
from xcube_clms.download_manager import DownloadTaskManager
from xcube_clms.processor import FileProcessor
from xcube_clms.token_handler import TokenHandler
from xcube_clms.utils import (
    spinner,
)


class PreloadData:
    def __init__(self, url, credentials, path):
        self._url: str = url
        self._credentials: dict = {}
        self.path: str = path

        self._task_control = {}
        self.cleanup: bool = True

        self._token_handler = TokenHandler(credentials)
        self._api_token: str = self._token_handler.api_token
        self._cache_manager = CacheManager(self.path)
        self._cache_manager.refresh_cache()
        self.file_store = self._cache_manager.get_file_store()
        self._file_processor = FileProcessor(self.path, self.file_store)
        self._download_manager = DownloadTaskManager(
            self._token_handler, self._url, self.path
        )

    def initiate_preload(self, data_id_maps: dict[str, Any]):
        self.refresh_cache()
        # We create a status event for each of the tasks so that we can
        # signal the thread that it can proceed further from the queue.
        for data_id_map_key in data_id_maps.keys():
            self._task_control[data_id_map_key] = {
                "status_event": threading.Event(),
            }

        executor = ThreadPoolExecutor()
        for data_id_map in data_id_maps.items():
            executor.submit(
                self._initiate_preload,
                data_id_map,
                self._task_control[data_id_map[0]]["status_event"],
            )

    def _initiate_preload(self, data_id_map, status_event):
        data_id = data_id_map[0]
        if data_id in self.view_cache().keys():
            LOG.info(f"The data for {data_id} is already cached at {self.path}")
            return

        task_id = self._download_manager.request_download(
            data_id=data_id,
            item=data_id_map[1].get("item"),
            product=data_id_map[1].get("product"),
        )

        spinner_thread = threading.Thread(
            target=spinner, args=(status_event, f"{task_id}")
        )
        status_event.set()
        spinner_thread.start()

        while status_event.is_set():
            status, _ = self._download_manager.get_current_requests_status(
                task_id=task_id
            )
            if status == COMPLETE:
                LOG.info(f"Status: {status} for {data_id}.")
                status_event.clear()
                spinner_thread.join()
                download_url, file_size = self._download_manager.get_download_url(
                    task_id
                )
                self.download_data(download_url, file_size, task_id, data_id)
                self._file_processor.postprocess(data_id)
            if status in PENDING:
                LOG.info(
                    f"Status: {status} for {data_id}. Will recheck status in "
                    f"{RETRY_TIMEOUT} seconds"
                )
            if status in CANCELLED:
                LOG.info(f"Status: {status} for {data_id}. Exiting now")
                status_event.clear()
                spinner_thread.join()
                return

            time.sleep(RETRY_TIMEOUT)

    def download_data(self, download_url, file_size, task_id, data_id):
        self._download_manager.download_data(download_url, file_size, task_id, data_id)

    def view_cache(self):
        return self._cache_manager.get_cache()

    def refresh_cache(self):
        return self._cache_manager.refresh_cache()
