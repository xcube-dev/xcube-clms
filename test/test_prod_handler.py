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

import unittest
from unittest.mock import Mock, patch

from xcube_clms.constants import (
    CLMS_DATA_ID_KEY,
    DATASET_DOWNLOAD_INFORMATION,
    FULL_SOURCE,
    ITEMS_KEY,
)
from xcube_clms.product_handler import ProductHandler
from xcube_clms.product_handlers.cdse import CdseProductHandler
from xcube_clms.product_handlers.eea import EeaProductHandler


class TestProductHandler(unittest.TestCase):
    def setUp(self):
        self.cache_store = Mock()
        self.api_token_handler = Mock()
        self.mock_credentials = {"secret": 42}
        self.datasets_info = [
            {
                CLMS_DATA_ID_KEY: "forest-type-2015",
                DATASET_DOWNLOAD_INFORMATION: {ITEMS_KEY: [{FULL_SOURCE: "eea"}]},
            },
            {
                CLMS_DATA_ID_KEY: "daily-surface-soil-moisture-v1.0",
                DATASET_DOWNLOAD_INFORMATION: {ITEMS_KEY: [{FULL_SOURCE: "cdse"}]},
            },
        ]

        mock_clms_token_handler_patcher = patch(
            "xcube_clms.product_handler.ClmsApiTokenHandler"
        )
        self.mock_clms_token_handler = mock_clms_token_handler_patcher.start()

    def tearDown(self):
        patch.stopall()

    def test_guess_eea(self):
        handler = ProductHandler.guess(
            data_id="forest-type-2015|FTY_2015_020m_eu_03035_d04_E00N20",
            datasets_info=self.datasets_info,
            cache_store=self.cache_store,
            credentials=self.mock_credentials,
        )
        self.assertIsInstance(handler, EeaProductHandler)

    def test_guess_cdse(self):
        handler = ProductHandler.guess(
            data_id="daily-surface-soil-moisture-v1.0",
            datasets_info=self.datasets_info,
            cache_store=self.cache_store,
            credentials=self.mock_credentials,
        )
        self.assertIsInstance(handler, CdseProductHandler)

    def test_guess_missing_args_raises(self):
        with self.assertRaises(ValueError) as context:
            ProductHandler.guess(
                data_id="forest-type-2015|FTY_2015_020m_eu_03035_d04_E00N20"
            )
        self.assertIn("All parameters are required", str(context.exception))

    def test_guess_unknown_id_raises(self):
        with self.assertRaises(ValueError) as context:
            ProductHandler.guess(
                data_id="unknown_id",
                datasets_info=self.datasets_info,
                cache_store=self.cache_store,
                credentials=self.mock_credentials,
            )
        self.assertIn("Unable to detect product handler", str(context.exception))

    def test_guess_unsupported_handler_raises(self):
        datasets_info = [
            {
                CLMS_DATA_ID_KEY: "product123",
                DATASET_DOWNLOAD_INFORMATION: {
                    ITEMS_KEY: [{FULL_SOURCE: "unknownsource"}]
                },
            }
        ]
        with self.assertRaises(ValueError) as context:
            ProductHandler.guess(
                data_id="product123",
                datasets_info=datasets_info,
                cache_store=self.cache_store,
                credentials=self.mock_credentials,
            )
        self.assertIn("currently not supported", str(context.exception))
