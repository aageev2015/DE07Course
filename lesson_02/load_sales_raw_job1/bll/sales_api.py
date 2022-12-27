from logs_handling.log_item import LogItemInterface


class SalesApiBlInterface(object):
    def __init__(self):
        pass

    @classmethod
    def save_sales_to_local_disk(self, log: LogItemInterface, date_str: str, raw_dir: str) -> None:
        """
        Fetch sales data from vendor on date. Place it locally

        Input parameters:
            date - date formatted in acceptable by vendor API format
            raw_dir - local relative path. Parent dir literal '..' not accepted
        """
        pass
