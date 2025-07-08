import unittest
from unittest.mock import Mock


from xcube_clms.constants import (
    CLMS_DATA_ID_KEY,
    DATASET_DOWNLOAD_INFORMATION,
    ITEMS_KEY,
    FULL_SOURCE,
)
from xcube_clms.product_handler import ProductHandler
from xcube_clms.product_handlers.eea import EeaProductHandler
from xcube_clms.product_handlers.legacy import LegacyProductHandler


class TestProductHandler(unittest.TestCase):

    def setUp(self):
        self.cache_store = Mock()
        self.api_token_handler = Mock()

        self.datasets_info = [
            {
                CLMS_DATA_ID_KEY: "forest-type-2015",
                DATASET_DOWNLOAD_INFORMATION: {ITEMS_KEY: [{FULL_SOURCE: "eea"}]},
            },
            {
                CLMS_DATA_ID_KEY: "daily-surface-soil-moisture-v1.0",
                DATASET_DOWNLOAD_INFORMATION: {ITEMS_KEY: [{FULL_SOURCE: "legacy"}]},
            },
        ]

    def test_guess_eea(self):
        handler = ProductHandler.guess(
            data_id="forest-type-2015|FTY_2015_020m_eu_03035_d04_E00N20",
            datasets_info=self.datasets_info,
            cache_store=self.cache_store,
            api_token_handler=self.api_token_handler,
        )
        self.assertIsInstance(handler, EeaProductHandler)

    def test_guess_legacy(self):
        handler = ProductHandler.guess(
            data_id="daily-surface-soil-moisture-v1.0",
            datasets_info=self.datasets_info,
            cache_store=self.cache_store,
            api_token_handler=self.api_token_handler,
        )
        self.assertIsInstance(handler, LegacyProductHandler)

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
                api_token_handler=self.api_token_handler,
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
                api_token_handler=self.api_token_handler,
            )
        self.assertIn("currently not supported", str(context.exception))
