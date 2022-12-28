"""
Integration tests for sales_raw_to_avro_job2 WebAPI controller
"""

import os
import shutil
from pathlib import Path

from unittest import TestCase

from sales_raw_to_avro_job2 import main


class WebAPIControllerTest(TestCase):
    WORKING_DIR = os.getcwd()
    TESTS_DIR = Path(__file__).parent

    @classmethod
    def setUpClass(cls) -> None:
        main.app.testing = True
        cls.client = main.app.test_client()

    @staticmethod
    def __prepare_test_file(file_from: str, file_to: str) -> None:
        if os.path.isfile(file_to):
            os.remove(file_to)
        shutil.copy2(file_from, file_to)

    @staticmethod
    def __remove_test_file(file_name: str) -> None:
        if os.path.isfile(file_name):
            os.remove(file_name)

    def __prepare_raw_files(self) -> None:
        self.__prepare_test_file(
            f'{self.TESTS_DIR}/data/sales_2022-08-09_1.json',
            f'{self.WORKING_DIR}/file_storage/raw/sales/2022-08-09/sales_2022-08-09_1.json')
        self.__prepare_test_file(
            f'{self.TESTS_DIR}/data/sales_2022-08-09_2.json',
            f'{self.WORKING_DIR}/file_storage/raw/sales/2022-08-09/sales_2022-08-09_2.json')

    def __remove_raw_files(self):
        self.__remove_test_file(f'{self.WORKING_DIR}/file_storage/raw/sales/2022-08-09/sales_2022-08-09_1.json')
        self.__remove_test_file(f'{self.WORKING_DIR}/file_storage/raw/sales/2022-08-09/sales_2022-08-09_2.json')

    @staticmethod
    def __clear_folder_files_only(clearing_folder_path):
        for file_path in os.listdir(clearing_folder_path):
            if os.path.isfile(file_path):
                os.remove(file_path)

    def test_sales_raw_to_avro_job2___when_happy_path_than_OK(self):
        # test only that avro files creatable. Not check result content
        self.__prepare_raw_files()

        resp = self.client.post(
            '/',
            json={
                "stg_dir": "/stg/sales/2022-08-09",
                "raw_dir": "/raw/sales/2022-08-09"
            },
        )

        self.assertEqual(201, resp.status_code)

        file1 = os.path.join(main.STG_SAVED_PATH, os.path.normpath('stg/sales/2022-08-09/sales_2022-08-09_1.avro'))
        file2 = os.path.join(main.STG_SAVED_PATH, os.path.normpath('stg/sales/2022-08-09/sales_2022-08-09_2.avro'))
        self.assertTrue(os.path.isfile(file1))
        self.assertTrue(os.path.isfile(file2))

        os.remove(file1)
        os.remove(file2)
        self.__remove_raw_files()

    def test_controller_load_sales_raw___when_not_supported_parameters_than_IM_A_TEAPOT(self):
        resp = self.client.post(
            '/',
            json={
                "stg_dir": "/stg/sales/2022-08-09",
                "raw_dir": "/raw/sales/2022-08-09",
                "cheat_code": "god_mode"
            },
        )

        self.assertEqual(418, resp.status_code)

    def test_sales_raw_to_avro_job2___when_data_to_convert_not_found_than_NOT_FOUND(self):
        self.__clear_folder_files_only(
            os.path.join(main.STG_SAVED_PATH, os.path.normpath('stg/sales/2022-08-09'))
        )

        resp = self.client.post(
            '/',
            json={
                "stg_dir": "/stg/sales/2022-08-09",
                "raw_dir": "/raw/sales/2022-08-09"
            },
        )

        self.assertEqual(404, resp.status_code)

    def test_controller_load_sales_raw___when_stg_dir_missed_than_BAD_REQUEST(self):
        resp = self.client.post(
            '/',
            json={
                "raw_dir": "/raw/sales/2022-08-09"
            },
        )

        self.assertEqual(400, resp.status_code)

    def test_controller_load_sales_raw___when_raw_dir_missed_than_BAD_REQUEST(self):
        resp = self.client.post(
            '/',
            json={
                "stg_dir": "/stg/sales/2022-08-09",
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
            "rew"
        ]:
            resp = self.client.post(
                '/',
                json={
                    "stg_dir": "/stg/sales/2022-08-09",
                    "raw_dir": wrong_path
                },
            )

            self.assertEqual(400, resp.status_code)

    # noinspection DuplicatedCode
    def test_sales_raw_to_avro_job2___when_json_date_format_wrong_than_UNPROCESSABLE_ENTITY(self):
        self.__prepare_test_file(
            f'{self.TESTS_DIR}/data/sales_2022-08-09_1_bad_date.json',
            f'{self.WORKING_DIR}/file_storage/raw/sales/2022-08-09/sales_2022-08-09_1.json')

        resp = self.client.post(
            '/',
            json={
                "stg_dir": "/stg/sales/2022-08-09",
                "raw_dir": "/raw/sales/2022-08-09"
            },
        )

        self.assertEqual(422, resp.status_code)

        file1 = os.path.join(main.STG_SAVED_PATH, os.path.normpath('stg/sales/2022-08-09/sales_2022-08-09_1.avro'))
        self.assertFalse(os.path.isfile(file1))

        self.__remove_test_file(f'{self.WORKING_DIR}/file_storage/raw/sales/2022-08-09/sales_2022-08-09_1.json')

    # noinspection DuplicatedCode
    def test_sales_raw_to_avro_job2___when_json_empty_client_than_UNPROCESSABLE_ENTITY(self):
        self.__prepare_test_file(
            f'{self.TESTS_DIR}/data/sales_2022-08-09_1_empty_client.json',
            f'{self.WORKING_DIR}/file_storage/raw/sales/2022-08-09/sales_2022-08-09_1.json')

        resp = self.client.post(
            '/',
            json={
                "stg_dir": "/stg/sales/2022-08-09",
                "raw_dir": "/raw/sales/2022-08-09"
            },
        )

        self.assertEqual(422, resp.status_code)

        file1 = os.path.join(main.STG_SAVED_PATH, os.path.normpath('stg/sales/2022-08-09/sales_2022-08-09_1.avro'))
        self.assertFalse(os.path.isfile(file1))

        self.__remove_test_file(f'{self.WORKING_DIR}/file_storage/raw/sales/2022-08-09/sales_2022-08-09_1.json')

    # noinspection DuplicatedCode
    def test_sales_raw_to_avro_job2___when_json_no_client_than_UNPROCESSABLE_ENTITY(self):
        self.__prepare_test_file(
            f'{self.TESTS_DIR}/data/sales_2022-08-09_1_no_client.json',
            f'{self.WORKING_DIR}/file_storage/raw/sales/2022-08-09/sales_2022-08-09_1.json')

        resp = self.client.post(
            '/',
            json={
                "stg_dir": "/stg/sales/2022-08-09",
                "raw_dir": "/raw/sales/2022-08-09"
            },
        )

        self.assertEqual(422, resp.status_code)

        file1 = os.path.join(main.STG_SAVED_PATH, os.path.normpath('stg/sales/2022-08-09/sales_2022-08-09_1.avro'))
        self.assertFalse(os.path.isfile(file1))

        self.__remove_test_file(f'{self.WORKING_DIR}/file_storage/raw/sales/2022-08-09/sales_2022-08-09_1.json')
