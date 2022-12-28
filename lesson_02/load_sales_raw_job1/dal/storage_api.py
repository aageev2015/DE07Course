from typing import List, Dict, Any

from logs_handling.log_item import LogItemInterface


class StorageDalInterface:
    def __init__(self):
        pass

    def save(self, log: LogItemInterface, json_content: List[Dict[str, Any]],
             logical_path: str, file_name: str) -> None:
        """
        Store list of objects to storage

        :param log: Log-item
        :param json_content: list of objects to storage
        :param logical_path: logical path
        :param file_name: file name
        """
        pass
