from typing import List, Dict, Any, Callable

from logs_handling.log_item import LogItemInterface


class StorageDalInterface:
    def __init__(self):
        pass

    def get_raw_files_paths(self, log: LogItemInterface, logical_path: str) -> List[str]:
        """
        Return list of file names of raw files by logical raw path. Non-recursive

        :param log: Log-item
        :param logical_path: logical path to find raw files
        :return: List of file names of raw files
        """
        pass

    def load_raw(self, log: LogItemInterface, logical_path: str, file_name: str) -> List[Dict[str, Any]]:
        """
        Load list of objects from raw storage

        :param log: Log-item
        :param logical_path: logical path to raw file
        :param file_name: raw file name
        :return: List of objects
        """
        pass

    def save_avro(self, log: LogItemInterface, raw_list: List[Dict[str, Any]],
                  logical_path: str, file_name: str,
                  record_raw_to_avro: Callable[[Dict[str, Any]], Dict[str, Any]]) -> None:
        """
        Store list of objects to stg storage

        :param log: Log-item
        :param raw_list: list of items to save
        :param logical_path: logical stg path
        :param file_name: stg file name
        :param record_raw_to_avro: raw to avro conversion logic
        """
        pass
