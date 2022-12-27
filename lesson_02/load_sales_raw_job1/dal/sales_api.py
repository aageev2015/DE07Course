from typing import List, Dict, Any

from logs_handling.log_item import LogItemInterface


class SalesDalInterface:
    def __init__(self):
        pass

    def convert_raw_to_avro(self, log: LogItemInterface, raw_dir: str, stg_dir: str) -> List[Dict[str, Any]]:
        """
        Get data from sales API for specified date.

        :param log: log-item for handling logs
        :param raw_dir: source folder with raw data
        :param stg_dir: target folder with converted avro data
        :return: list of records
        """
        pass
