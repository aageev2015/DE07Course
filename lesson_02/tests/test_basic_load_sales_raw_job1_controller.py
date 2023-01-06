"""
Integration tests for load_sales_raw_job1 WebAPI controller
"""

import os
import glob
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

        self.assertTrue(self.__is_raw_file('raw/sales/2022-08-09/sales_2022-08-09_1.json'))
        self.assertTrue(self.__is_raw_file('raw/sales/2022-08-09/sales_2022-08-09_2.json'))
        self.assertFalse(self.__is_raw_file('raw/sales/2022-08-09/sales_2022-08-09_3.json'))

        os.remove(self.__raw_file_full_path('raw/sales/2022-08-09/sales_2022-08-09_1.json'))
        os.remove(self.__raw_file_full_path('raw/sales/2022-08-09/sales_2022-08-09_2.json'))

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

    @responses.activate
    def test_controller_load_sales_raw___when_any_fail_middle_of_load_than_roll_back_as_default(self):
        def _post(date_str: str):
            return self.client.post(
                '/',
                json={
                    "date": date_str,
                    "raw_dir": f"/raw/sales/{date_str}"
                },
            )

        self.__prepare_vendor_api_mock_as_failing_in_middle_sample1("2022-08-08", HTTPStatus.OK)
        _post("2022-08-08")
        self.assertTrue(self.__is_raw_file('raw/sales/2022-08-08/sales_2022-08-08_1.json'))
        self.assertTrue(self.__is_raw_file('raw/sales/2022-08-08/sales_2022-08-08_2.json'))
        self.assertTrue(self.__is_raw_file('raw/sales/2022-08-08/sales_2022-08-08_3.json'))
        self.assertFalse(self.__is_raw_file('raw/sales/2022-08-08/sales_2022-08-08_4json'))

        self.__prepare_vendor_api_mock_as_failing_in_middle_sample1("2022-08-09", HTTPStatus.REQUEST_TIMEOUT)
        _post("2022-08-09")
        self.assertFalse(self.__is_raw_file('raw/sales/2022-08-09/sales_2022-08-09_1.json'))
        self.assertFalse(self.__is_raw_file('raw/sales/2022-08-09/sales_2022-08-09_2.json'))
        self.assertFalse(self.__is_raw_file('raw/sales/2022-08-09/sales_2022-08-09_3.json'))

        self.__prepare_vendor_api_mock_as_failing_in_middle_sample1("2022-08-10", HTTPStatus.INTERNAL_SERVER_ERROR)
        _post("2022-08-10")
        self.assertFalse(self.__is_raw_file('raw/sales/2022-08-10/sales_2022-08-10_1.json'))
        self.assertFalse(self.__is_raw_file('raw/sales/2022-08-10/sales_2022-08-10_2.json'))
        self.assertFalse(self.__is_raw_file('raw/sales/2022-08-10/sales_2022-08-10_3.json'))

        self.__prepare_vendor_api_mock_as_failing_in_middle_sample1("2022-08-11", HTTPStatus.BAD_REQUEST)
        _post("2022-08-11")
        self.assertFalse(self.__is_raw_file('raw/sales/2022-08-11/sales_2022-08-11_1.json'))
        self.assertFalse(self.__is_raw_file('raw/sales/2022-08-11/sales_2022-08-11_2.json'))
        self.assertFalse(self.__is_raw_file('raw/sales/2022-08-11/sales_2022-08-11_3.json'))

        self.__prepare_vendor_api_mock_as_failing_in_middle_sample1("2022-08-12", HTTPStatus.IM_USED)
        _post("2022-08-12")
        self.assertFalse(self.__is_raw_file('raw/sales/2022-08-12/sales_2022-08-12_1.json'))
        self.assertFalse(self.__is_raw_file('raw/sales/2022-08-12/sales_2022-08-12_2.json'))
        self.assertFalse(self.__is_raw_file('raw/sales/2022-08-12/sales_2022-08-12_3.json'))

    @staticmethod
    def __raw_file_full_path(logical_path: str):
        return os.path.join(main.RAW_LOADED_PATH, os.path.normpath(logical_path))

    @staticmethod
    def __is_raw_file(logical_path: str):
        file_path = WebAPIControllerTest.__raw_file_full_path(logical_path)
        return os.path.isfile(file_path)

    @staticmethod
    def __prepare_vendor_api_mock_as_failing_in_middle_sample1(date_str: str, fail_response_status: HTTPStatus):
        responses.add(
            responses.GET,
            f'http://sales-tests-api.com/sales?date={date_str}&page=1',
            json=[
                {"client": "Sean Davis", "purchase_date": date_str, "product": "Laptop", "price": 957},
                {"client": "Sean Davis", "purchase_date": date_str, "product": "Phone", "price": 1139},
                {"client": "Jonathan Briggs", "purchase_date": date_str, "product": "Microwave oven", "price": 205},
            ], status=HTTPStatus.OK)
        responses.add(
            responses.GET,
            f'http://sales-tests-api.com/sales?date={date_str}&page=2',
            json=[{"client": "Daniel Holder", "purchase_date": date_str, "product": "TV", "price": 1220}],
            status=fail_response_status)
        responses.add(
            responses.GET,
            f'http://sales-tests-api.com/sales?date={date_str}&page=3',
            json=[
                {"client": "Elizabeth Jackson", "purchase_date": "2022-08-09", "product": "Phone", "price": 1415},
                {"client": "Michael Myers", "purchase_date": date_str, "product": "TV", "price": 1697}
            ], status=HTTPStatus.OK)
        responses.add(
            responses.GET,
            f'http://sales-tests-api.com/sales?date={date_str}&page=4',
            status=HTTPStatus.NOT_FOUND)
