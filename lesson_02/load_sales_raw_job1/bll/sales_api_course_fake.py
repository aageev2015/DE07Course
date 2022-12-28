import time

from load_sales_raw_job1.bll.exceptions import *
from load_sales_raw_job1.bll.sales_api import SalesApiBlInterface
from load_sales_raw_job1.dal.exceptions import *
from load_sales_raw_job1.dal.sales_api import SalesDalInterface
from load_sales_raw_job1.dal.storage_api import StorageDalInterface
from logs_handling.log_item import LogItemInterface
from support_tools.basic_validators import BasicDateValidator, RelativeFilePathValidator, RawFilePathValidator


class SalesApiBllCourseFake(SalesApiBlInterface):
    FIRST_PAGE: int = 1

    def __init__(self, sales_api_dal: SalesDalInterface, storage_api_dal: StorageDalInterface,
                 continue_on_page_load_fail: bool,
                 requests_delay_sec: float,
                 request_max_fails_count: int,
                 file_name_template: str, ):
        super(SalesApiBllCourseFake, self).__init__()
        self.__date_validator = BasicDateValidator()
        self.__path_validator_raw = RawFilePathValidator()
        self.__path_validator_relative = RelativeFilePathValidator()
        self.__sales_api_dal = sales_api_dal
        self.__storage_api_dal = storage_api_dal
        self.__continue_on_page_load_fail = continue_on_page_load_fail
        self.__requests_delay_sec = requests_delay_sec
        self.__request_max_fails_count = request_max_fails_count
        self.__file_name_template = file_name_template

    def save_sales_to_storage(self, log: LogItemInterface, date_str: str, raw_dir: str) -> None:
        """
        Fetch sales data from vendor API on date. Save it to storage

        Input parameters:
            :param log: log-item for handling logs: "2022-12-09"
            :param date_str: date formatted in acceptable by vendor API format: "/raw/sales/2022-12-09"
            :param raw_dir: logical raw path
        """

        date_str = date_str.strip()
        raw_dir = raw_dir.strip()

        log.dev_debug(f"Start job1 on '{date_str}' to '{raw_dir}'")
        if not self.__date_validator.validate(date_str):
            raise SalesBllDateFormatException(
                f"Date format is wrong. Must be '{self.__date_validator.format}'. date_str='{date_str}'")
        if not self.__path_validator_raw.validate(raw_dir) or not self.__path_validator_relative.validate(raw_dir):
            raise SalesBllPathFormatException(
                f"Raw path format is wrong. "
                f"Must be '{self.__path_validator_raw.format}' and '{self.__path_validator_relative.format}'. "
                f"raw_dir='{raw_dir}")

        page_num = self.FIRST_PAGE
        page_fail_count = 0
        total_records_count = 0
        while True:
            try:
                log.dev_debug(f"BLL. Loading page {page_num}")
                sales_list = self.__sales_api_dal.get_sales(log, date_str, page_num)

                records_count = len(sales_list)
                if records_count == 0:
                    log.dev_debug(f"BLL. Page {page_num} empty")
                    break

                log.dev_debug(f"BLL. Saving page {page_num}")
                file_name = self.__file_name_template \
                    .replace("%date%", date_str) \
                    .replace("%page%", str(page_num))
                self.__storage_api_dal.save(log, sales_list, raw_dir, file_name)

                total_records_count = total_records_count + records_count
            except BaseException as e:
                page_fail_count = page_fail_count + 1
                self.__handle_exception_per_page(e, log)

            if page_fail_count > self.__request_max_fails_count:
                break
            page_num = page_num + 1
            time.sleep(self.__requests_delay_sec)

        page_tries_count = page_num + 1
        loaded_count = page_tries_count - page_fail_count
        log.dev_debug(f"BLL. Done job1. Loaded {loaded_count} of {page_tries_count}. "
                      f"{total_records_count} total records loaded")
        if page_fail_count > 0 and loaded_count > 0:
            raise SalesBllPartiallyLoadedException(f"{loaded_count} of {page_tries_count} was loaded")
        elif page_fail_count > 0 and loaded_count == 0:
            raise SalesBllAllTriesFailedException()
        elif total_records_count == 0:
            raise SalesBllNothingLoadedException()

    def __handle_exception_per_page(self, e: BaseException, log: LogItemInterface) -> None:
        if not self.__continue_on_page_load_fail:
            raise e

        match e:
            case SalesDalAPIJSONFormatException():
                log.dev_error(e, "Vendor's sales API return wrong JSON format")
            case SalesDalAPIRequestFailedTemporaryException():
                log.dev_error(e, "Vendor's sales API request failed")
            case SalesDalAPIRequestFailedException():
                raise e
            case SalesDalStorageSaveException():
                raise e
            case _:
                raise e
