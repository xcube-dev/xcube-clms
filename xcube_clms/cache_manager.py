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

from xcube.core.store import new_data_store

from xcube_clms.constants import LOG, DATA_ID_SEPARATOR


class CacheManager:
    def __init__(self, path):
        self.cache = None
        self.path = path
        os.makedirs(self.path, exist_ok=True)
        LOG.info(f"Local Filestore for preload cache created at {self.path}")
        self.file_store = new_data_store("file", root=self.path)
        self.refresh_cache()

    def refresh_cache(self):
        self.cache = {
            d: os.path.join(self.path, d)
            for d in os.listdir(self.path)
            if DATA_ID_SEPARATOR in d
        }

    def get_file_store(self):
        return self.file_store

    def get_cache(self):
        return self.cache
