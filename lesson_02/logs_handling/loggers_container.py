from datetime import datetime
import logging.config
from logging import Logger


class LoggersContainer:
    """
    Container for separated loggers:
     client or user - messages and errors with origin client's data sent to this service
     developer - for debug and monitoring service stability by developer or admin
    """
    def __init__(self, ini_path: str):
        logging.config.fileConfig(f'{ini_path}/logs.ini',
                                  defaults={'date': datetime.now().strftime('%Y_%m_%d')})
        self.__cli = logging.getLogger('client')
        self.__cli.fatal("This log starts")
        self.__dev = logging.getLogger('developer')
        self.__dev.fatal("This log starts")

    @property
    def cli(self) -> Logger:
        return self.__cli

    @property
    def dev(self) -> Logger:
        return self.__dev

    def terminate(self, reason: str = 'Undefined') -> None:
        """
        Logging service stopped.
        Fatal level used to guarantee than this message will be written independently of configured log-level
        :param reason:
        :return:
        """
        print(f'Service stopping with reason {reason}')
        self.__cli.fatal(f'Service stopping with reason {reason}')
        self.__dev.fatal(f'Service stopping with reason {reason}')
