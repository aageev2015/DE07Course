"""
Integration tests for load_sales_raw_job1 WebAPI controller modes based on settings
"""

from unittest import TestCase

from load_sales_raw_job1 import main


class WebAPIControllerWorkingModesTest(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        main.app.testing = True
        cls.client = main.app.test_client()
        main.sales_dal._SalesDalCourseFake__api_url = 'http://sales-tests-api.com/sales'
        main.sales_bll._SalesApiBllCourseFake__requests_delay_sec = 0