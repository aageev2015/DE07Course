import json
from typing import List, Dict, Any, Callable

import avro
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

from sales_raw_to_avro_job2.dal.exceptions import *
from sales_raw_to_avro_job2.dal.storage_api import StorageDalInterface
from logs_handling.log_item import LogItemInterface
from support_tools.file_tools import *
from logs_handling.log_formatter import LogFormatter


class StorageDalDisk(StorageDalInterface):
    def __init__(self, root_stg_path: str, root_raw_path: str, avro_schema_config_file: str):
        super(StorageDalDisk, self).__init__()
        self.__root_stg_path = os.path.normpath(os.path.normcase(root_stg_path))
        self.__root_raw_path = os.path.normpath(os.path.normcase(root_raw_path))
        self.__avro_schema = avro.schema.from_path(avro_schema_config_file)

    def get_raw_files_paths(self, log: LogItemInterface, logical_path: str) -> List[str]:
        """
        Return list of file names of raw files by logical raw path. Non-recursive

        :param log: Log-item
        :param logical_path: logical path to find raw files
        :return: List of file names of raw files
        """
        relative_path = logical_path[1:] if logical_path[0] in ["/", "\\"] else logical_path
        norm_path = os.path.normpath(os.path.normcase(relative_path))
        full_path = os.path.join(self.__root_raw_path, norm_path)
        file_names = [
            file_name
            for file_name in os.listdir(full_path)
            if os.path.isfile(full_path + os.sep + file_name)]
        return file_names

    def load_raw(self, log: LogItemInterface, logical_path: str, file_name: str) -> List[Dict[str, Any]]:
        """
        Load list of objects from raw storage

        :param log: Log-item
        :param logical_path: logical path to raw file
        :param file_name: raw file name
        :return: List of objects
        """

        full_file_path = logical_to_physical_file_path(self.__root_raw_path, logical_path, file_name)
        try:
            with open(full_file_path, "r") as file:
                data = json.load(file)
            return data
        except BaseException as e:
            raise SalesDalStorageLoadRawException(
                LogFormatter.format_except(e, f"Load raw JSON from file failed. {full_file_path}"))

    def save_avro(self, log: LogItemInterface, raw_list: List[Dict[str, Any]],
                  logical_path: str, file_name: str,
                  record_raw_to_avro: Callable[[Dict[str, Any]], Dict[str, Any]]) -> None:
        """
        Store list of objects to stg storage

        :param log: Log-item
        :param raw_list: list of items to save
        :param logical_path: logical stg path
        :param file_name: stg file name
        :param record_raw_to_avro: logic of conversion json to avro
        """

        full_file_path = logical_to_physical_file_path(self.__root_stg_path, logical_path, file_name)
        tmp_full_file_path = full_file_path + '.tmp'
        full_folder_path = os.path.dirname(full_file_path)
        guarantee_folder_exists(full_folder_path)
        try:
            if os.path.isfile(full_file_path):
                os.remove(full_file_path)
            if os.path.isfile(tmp_full_file_path):
                os.remove(tmp_full_file_path)

            with open(tmp_full_file_path, 'wb') as file:
                avro_writer = DataFileWriter(file, DatumWriter(), self.__avro_schema)
                for raw_record in raw_list:
                    # any record with wrong format must throw exception
                    avro_record = record_raw_to_avro(raw_record)
                    avro_writer.append(avro_record)
                avro_writer.close()

            if os.path.isfile(full_file_path):
                os.remove(full_file_path)
            os.rename(tmp_full_file_path, full_file_path)

        except (SalesDalAvroDateFormatException, SalesDalAvroKeyErrorException) as e:
            if os.path.isfile(tmp_full_file_path):
                os.remove(tmp_full_file_path)
            if os.path.isfile(full_file_path):
                os.remove(full_file_path)
            raise e
        except BaseException as e:
            if os.path.isfile(tmp_full_file_path):
                os.remove(tmp_full_file_path)
            if os.path.isfile(full_file_path):
                os.remove(full_file_path)
            raise SalesDalStorageSaveAvroException(
                LogFormatter.format_except(e, f"Saving to avro file failed. {full_file_path}"))

    def remove_avro(self, log: LogItemInterface, logical_path: str, file_name: str):
        full_file_path = logical_to_physical_file_path(self.__root_stg_path, logical_path, file_name)
        if os.path.isfile(full_file_path):
            os.remove(full_file_path)
