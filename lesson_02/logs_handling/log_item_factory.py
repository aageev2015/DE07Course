from logs_handling.log_formatter import LogFormatter
from logs_handling.log_item import LogItemForRequest, LogItemInterface, LogItemGeneral
from logs_handling.loggers_container import LoggersContainer
from support_tools.req_id_generator import ReqIdGeneratorInterface


class LogItemFactory:
    def __init__(self, log_container: LoggersContainer, req_id_generator: ReqIdGeneratorInterface):
        self.__log_container = log_container
        self.__req_id_generator = req_id_generator
        try:
            self.new_for_request()
            self.new_general()
        except BaseException as e:
            print('LogItemFactory creation failed')
            log_container.dev.fatal(LogFormatter.format_except(e, "LogItemFactory creation failed"))

    def new_for_request(self) -> LogItemInterface:
        return LogItemForRequest(self.__log_container, self.__req_id_generator.gen())

    def new_general(self) -> LogItemInterface:
        return LogItemGeneral(self.__log_container)
