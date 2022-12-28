from logs_handling.log_item import LogItemInterface


class SalesApiBlInterface(object):
    def __init__(self):
        pass

    def convert_raw_to_avro(self, log: LogItemInterface, stg_dir: str, raw_dir: str) -> None:
        """
        Get data from sales API for specified date.

        :param log: log-item for handling logs
        :param stg_dir: target logical folder for target converted avro data
        :param raw_dir: source logical folder with source raw data
        """
        pass
