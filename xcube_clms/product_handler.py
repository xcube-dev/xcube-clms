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

from abc import ABC, abstractmethod
from typing import Any

from xcube.core.store import DataOpener, DataPreloader, PreloadHandle
from xcube.core.store.preload import NullPreloadHandle, ExecutorPreloadHandle
from xcube.util.jsonschema import JsonObjectSchema


class NullDownloader:
    @abstractmethod
    def prepare_request(self, data_id):
        pass

    @abstractmethod
    def request_download(self, data_id):
        pass

    @abstractmethod
    def download_data(self, data_id):
        """To be implemented when preloading is required"""
        pass


class NullProcessor:
    @abstractmethod
    def preprocess_data(self, data_id):
        pass


class ProductHandler(DataOpener, DataPreloader, ABC):
    """Product handler class to handle various sources of products from the
    CLMS API
    """

    def __init__(self, downloader=None, preprocessor=None):
        self.downloader = downloader or NullDownloader()
        self.preprocessor = preprocessor or NullProcessor()

    @classmethod
    def guess(cls, data_id: str) -> "ProductHandler":
        """Guess the suitable product handler for the data id requested.

        Args:
            data_id: Data identifier of the data source.

        Returns:
            The product handler.
        Raises:
            ValueError: if guessing the product handler failed.
        """

    @abstractmethod
    def get_open_data_params_schema(self, data_id: str = None) -> JsonObjectSchema:
        pass

    @abstractmethod
    def open_data(self, data_id: str, **open_params) -> Any:
        pass

    @property
    @abstractmethod
    def product_type(self):
        pass

    def preload_data(
        self,
        *data_ids: str,
        **preload_params: Any,
    ) -> PreloadHandle:
        return NullPreloadHandle()

    def get_preload_data_params_schema(self) -> JsonObjectSchema:
        return JsonObjectSchema(additional_properties=False)


#######################


class DemoProdHandler(ProductHandler):
    def __init__(self, downloader=None, preprocessor=None):
        super.__init__(downloader, preprocessor)

    def preload_data(
        self,
        *data_ids: str,
        **preload_params: Any,
    ) -> PreloadHandle:
        return ClmsPreloadHandle(
            data_id_maps=data_id_maps,
            url=CLMS_API_URL,
            credentials=self.credentials,
            cache_store=self.cache_store,
            downloader=downloader,
            processor=processor,
            **preload_params,
        )


downloader = NullDownloader()
processor = NullProcessor()


class ClmsPreloadHandle(ExecutorPreloadHandle):
    def __init__(
        self,
        downloader: NullDownloader | None = None,
        processor: NullProcessor | None = None,
    ):
        self.downloader = downloader
        self.processor = processor

    def preload_data(self, data_id: str):
        self.downloader.prepare_request()
        self.downloader.request_download()
        self.downloader.download_data()
        self.processor.preprocess_data()


demo1 = DemoProdHandler(Downloader, processor)


class Demo2ProdHandler(ProductHandler):
    def __init__(self, downloader=None, preprocessor=None):
        """This handler does not need preprocessor"""
        super.__init__(downloader, preprocessor)

    def open_data(self):
        self.downloader.prepare_request()
        links = self.downloader.request_download()
        filtered = self.filter_links(links)
        return xr.open_mfdata(filtered, engine="h5....")


demo2 = Demo2ProdHandler("downloader")
