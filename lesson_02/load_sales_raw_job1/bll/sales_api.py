from logs_handling.log_item import LogItemInterface


class SalesApiBlInterface(object):
    def __init__(self):
        pass

    def save_sales_to_storage(self, log: LogItemInterface, date_str: str, raw_dir: str) -> None:
        """
        Fetch sales data from vendor on date. Save it to storage

        Input parameters:
            :param log: log-item for handling logs
            :param date_str: date formatted in acceptable by vendor API format
            :param raw_dir: logical raw path
        """
        pass
