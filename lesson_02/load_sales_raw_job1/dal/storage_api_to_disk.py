import json
import os
from typing import List, Dict, Any

from load_sales_raw_job1.dal.exceptions import SalesDalStorageSaveException
from load_sales_raw_job1.dal.storage_api import StorageDalInterface
from logs_handling.log_item import LogItemInterface
from support_tools.file_tools import guarantee_folder_exists, logical_to_physical_file_path
from logs_handling.log_formatter import LogFormatter


class StorageDalDisk(StorageDalInterface):
    def __init__(self, root_path: str):
        super(StorageDalDisk, self).__init__()
        self.__root_path = os.path.normpath(os.path.normcase(root_path))

    def save(self, log: LogItemInterface, json_content: List[Dict[str, Any]],
             logical_path: str, file_name: str) -> None:
        full_file_path = logical_to_physical_file_path(self.__root_path, logical_path, file_name)
        full_folder_path = os.path.dirname(full_file_path)
        guarantee_folder_exists(full_folder_path)
        try:
            if os.path.isfile(full_file_path):
                os.remove(full_file_path)
            with open(full_file_path, "w") as file:
                json.dump(json_content, file)
        except BaseException as e:
            raise SalesDalStorageSaveException(
                LogFormatter.format_except(e, f"Saving to file failed. {full_file_path}"))
