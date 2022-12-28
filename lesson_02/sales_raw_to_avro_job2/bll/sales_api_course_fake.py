import re
from typing import Dict, Any

from sales_raw_to_avro_job2.bll.exceptions import *
from sales_raw_to_avro_job2.dal.exceptions import SalesDalAvroDateFormatException, SalesDalAvroKeyErrorException
from sales_raw_to_avro_job2.bll.sales_api import SalesApiBlInterface
from sales_raw_to_avro_job2.dal.storage_api import StorageDalInterface
from logs_handling.log_item import LogItemInterface
from support_tools.basic_validators import *


class SalesApiBllCourseFake(SalesApiBlInterface):
    def __init__(self, storage_api_dal: StorageDalInterface,
                 stg_file_name_regex: str,
                 raw_file_name_regex: str):
        super(SalesApiBllCourseFake, self).__init__()
        self.__date_validator = BasicDateValidator()
        self.__path_validator_stg = StgFilePathValidator()
        self.__path_validator_raw = RawFilePathValidator()
        self.__path_validator_relative = RelativeFilePathValidator()
        self.__storage_api_dal = storage_api_dal
        self.__stg_file_name_regex = stg_file_name_regex
        self.__raw_file_name_regex = re.compile(raw_file_name_regex)
        self.__required_record_properties = {"client", "purchase_date", "product", "price"}

    def convert_raw_to_avro(self, log: LogItemInterface, stg_dir: str, raw_dir: str) -> None:
        """
        Get data from sales API for specified date.

        :param log: log-item for handling logs
        :param stg_dir: target logical folder for target converted avro data. "/stg/sales/2022-12-09"
        :param raw_dir: source logical folder with source raw data. "/raw/sales/2022-12-09"
        """

        stg_dir = stg_dir.strip()
        raw_dir = raw_dir.strip()

        log.dev_debug(f"Start job2 conversion from raw '{raw_dir}' to avro '{stg_dir}'")
        if not self.__path_validator_stg.validate(stg_dir) or not self.__path_validator_relative.validate(stg_dir):
            raise SalesBllPathFormatException(
                f"Avro path format is wrong. "
                f"Must be '{self.__path_validator_stg.format}' and '{self.__path_validator_relative.format}'. "
                f"stg_dir='{stg_dir}")
        if not self.__path_validator_raw.validate(raw_dir) or not self.__path_validator_relative.validate(raw_dir):
            raise SalesBllPathFormatException(
                f"Raw path format is wrong. "
                f"Must be '{self.__path_validator_raw.format}' and '{self.__path_validator_relative.format}'. "
                f"raw_dir='{raw_dir}")

        log.dev_debug(f"BLL. Loading raw file names")
        raw_file_names = self.__storage_api_dal.get_raw_files_paths(log, raw_dir)
        log.dev_debug(f"BLL. {len(raw_file_names)} files was found")

        files_processed_count = 0
        for raw_file_name in raw_file_names:
            stg_file_name = self.__raw_file_name_regex.sub(self.__stg_file_name_regex, raw_file_name)
            if raw_file_name == stg_file_name:
                log.dev_debug(f"BLL. raw file {stg_file_name} skipped by file name mismatch")
                continue
            raw_data = self.__storage_api_dal.load_raw(log, raw_dir, raw_file_name)
            self.__storage_api_dal.save_avro(log, raw_data, stg_dir, stg_file_name, self.__raw_record_to_avro)
            files_processed_count = files_processed_count + 1
            log.dev_debug(f"BLL. {raw_file_name} converted to {stg_file_name}")

        log.dev_debug(f"BLL. Conversion finished. {files_processed_count} files processed")

        if files_processed_count == 0:
            raise SalesBllRawFolderNotFoundOrEmptyException()

    def __raw_record_to_avro(self, raw_record: Dict[str, Any]) -> Dict[str, Any]:
        # implemented just to convert date correctly. Not found how do this on avro schema level
        # same about required. must be by avro
        prop_keys = raw_record.keys()
        empty_props = {prop_name for prop_name in prop_keys if raw_record[prop_name] == ""}
        missed_props = self.__required_record_properties - prop_keys
        empty_missed_props = empty_props.union(missed_props)
        if len(empty_missed_props) > 0:
            raise SalesDalAvroKeyErrorException(f"Required properties not found in raw data: {empty_missed_props}")

        purchase_date = raw_record["purchase_date"]
        try:
            purchase_date_parsed = datetime.datetime.strptime(purchase_date, "%Y-%m-%d")
        except ValueError as e:
            raise SalesDalAvroDateFormatException(f"purchase_date value '{purchase_date}' not parsed: {e}")
        purchase_date = purchase_date_parsed.date()

        return {
            "client": raw_record.get("client", ""),
            "purchase_date": purchase_date,
            "product": raw_record.get("product", ""),
            "price": raw_record.get("price", 0)
        }
