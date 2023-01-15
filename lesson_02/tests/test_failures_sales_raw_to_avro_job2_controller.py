"""
Integration tests for sales_raw_to_avro_job2 WebAPI controller failures
"""

from unittest import TestCase

from load_sales_raw_job1 import main


class WebAPIControllerFailuresTest(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        main.app.testing = True
        cls.client = main.app.test_client()
        main.sales_dal._SalesDalCourseFake__api_url = 'http://sales-tests-api.com/sales'
        main.sales_bll._SalesApiBllCourseFake__requests_delay_sec = 0
