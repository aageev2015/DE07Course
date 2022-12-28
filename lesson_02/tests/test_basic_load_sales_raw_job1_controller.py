"""
Integration tests for load_sales_raw_job1 WebAPI controller
"""

import os
from http import HTTPStatus
import responses
from unittest import TestCase

from load_sales_raw_job1 import main


class WebAPIControllerTest(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        main.app.testing = True
        cls.client = main.app.test_client()
        main.sales_dal._SalesDalCourseFake__api_url = 'http://sales-tests-api.com/sales'
        main.sales_bll._SalesApiBllCourseFake__requests_delay_sec = 0

    @responses.activate
    def test_controller_load_sales_raw___when_happy_path_than_OK(self):
        responses.add(
            responses.GET,
            'http://sales-tests-api.com/sales?date=2022-08-09&page=1',
            json=[
                {"client": "Sean Davis", "purchase_date": "2022-08-09", "product": "Laptop", "price": 957},
                {"client": "Sean Davis", "purchase_date": "2022-08-09", "product": "Phone", "price": 1139},
                {"client": "Jonathan Briggs", "purchase_date": "2022-08-09", "product": "Microwave oven", "price": 205},
                {"client": "Ashley Smith", "purchase_date": "2022-08-09", "product": "TV", "price": 1554},
                {"client": "Taylor Hines", "purchase_date": "2022-08-09", "product": "Laptop", "price": 2751},
                {"client": "Robert Mclean", "purchase_date": "2022-08-09", "product": "coffee machine", "price": 675},
                {"client": "Katie Nguyen", "purchase_date": "2022-08-09", "product": "coffee machine", "price": 513},
                {"client": "Antonio Cohen", "purchase_date": "2022-08-09", "product": "coffee machine", "price": 645},
                {"client": "Elizabeth Jackson", "purchase_date": "2022-08-09", "product": "Phone", "price": 1415},
                {"client": "Daniel Holder", "purchase_date": "2022-08-09", "product": "TV", "price": 1220}
            ], status=HTTPStatus.OK)
        responses.add(
            responses.GET,
            'http://sales-tests-api.com/sales?date=2022-08-09&page=2',
            json=[
                {"client": "Michael Myers", "purchase_date": "2022-08-09", "product": "TV", "price": 1697}
            ], status=HTTPStatus.OK)
        responses.add(
            responses.GET,
            'http://sales-tests-api.com/sales?date=2022-08-09&page=3',
            status=HTTPStatus.NOT_FOUND)

        resp = self.client.post(
            '/',
            json={
                "date": "2022-08-09",
                "raw_dir": "/raw/sales/2022-08-09"
            },
        )

        self.assertEqual(201, resp.status_code)

        file1 = os.path.join(main.RAW_LOADED_PATH, os.path.normpath('raw/sales/2022-08-09/sales_2022-08-09_1.json'))
        file2 = os.path.join(main.RAW_LOADED_PATH, os.path.normpath('raw/sales/2022-08-09/sales_2022-08-09_2.json'))
        file3 = os.path.join(main.RAW_LOADED_PATH, os.path.normpath('raw/sales/2022-08-09/sales_2022-08-09_3.json'))
        self.assertTrue(os.path.isfile(file1))
        self.assertTrue(os.path.isfile(file2))
        self.assertFalse(os.path.isfile(file3))

        os.remove(file1)
        os.remove(file2)

    @responses.activate
    def test_controller_load_sales_raw___when_no_data_loaded_than_NOT_FOUND(self):
        # case: vendor service returns NOT_FOUND
        responses.add(
            responses.GET,
            'http://sales-tests-api.com/sales?date=2022-08-09&page=1',
            status=HTTPStatus.NOT_FOUND)

        resp = self.client.post(
            '/',
            json={
                "date": "2022-08-09",
                "raw_dir": "/raw/sales/2022-08-09"
            },
        )

        self.assertEqual(404, resp.status_code)

        # case: vendor service returns OK with empty data
        responses.add(
            responses.GET,
            'http://sales-tests-api.com/sales?date=2022-08-09&page=1',
            json=[],
            status=HTTPStatus.OK)

        resp = self.client.post(
            '/',
            json={
                "date": "2022-08-09",
                "raw_dir": "/raw/sales/2022-08-09"
            },
        )

        self.assertEqual(404, resp.status_code)

    def test_controller_load_sales_raw___when_not_supported_parameters_than_IM_A_TEAPOT(self):
        resp = self.client.post(
            '/',
            json={
                "date": "2022-08-09",
                "raw_dir": "/raw/sales/2022-08-09",
                "cheat_code": "god_mode"
            },
        )

        self.assertEqual(418, resp.status_code)

    def test_controller_load_sales_raw___when_date_missed_than_BAD_REQUEST(self):
        resp = self.client.post(
            '/',
            json={
                "raw_dir": "/raw/sales/2022-08-09",
            },
        )

        self.assertEqual(400, resp.status_code)

    def test_controller_load_sales_raw___when_raw_dir_missed_than_BAD_REQUEST(self):
        resp = self.client.post(
            '/',
            json={
                "date": "2022-08-09",
            },
        )

        self.assertEqual(400, resp.status_code)

    def test_controller_load_sales_raw___when_raw_dir_wrong_format_than_BAD_REQUEST(self):
        for wrong_path in [
            # contains parents
            "..", "raw/..", "raw/../sales", "raw/sales/../../..", "raw/sales/.."
            # is absolute
            "//raw/sales", "c:/raw/sales"
            # not starts with raw
            "", "/rew/sales/2022-08-09"
        ]:
            resp = self.client.post(
                '/',
                json={
                    "date": "2022-08-09",
                    "raw_dir": wrong_path,
                },
            )
            self.assertEqual(400, resp.status_code)
