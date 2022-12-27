from typing import List, Dict, Any

from logs_handling.log_item import LogItemInterface


class SalesDalInterface:
    def __init__(self):
        pass

    def get_sales(self, log: LogItemInterface, date_str: str, page_num: int) -> List[Dict[str, Any]]:
        """
        Get data from sales API for specified date and page.

        :param log: log-item for handling logs
        :param date_str: date filter
        :param page_num: page number filter
        :return: list of records
        """
        pass
