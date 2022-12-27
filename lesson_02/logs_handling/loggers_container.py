from datetime import datetime
import logging.config
from logging import Logger


class LoggersContainer:
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
        print(f'Service stopping with reason {reason}')
        self.__cli.fatal(f'Service stopping with reason {reason}')
