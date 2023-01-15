from multipledispatch import dispatch

from logs_handling.log_formatter import LogFormatter
from logs_handling.loggers_container import LoggersContainer


class LogItemInterface:
    @dispatch(str)
    def user_error(self, error_msg: str) -> None:
        pass

    @dispatch(str)
    def user_error(self, e: BaseException, error_msg: str) -> None:
        pass

    @dispatch(BaseException, str)
    def user_warning(self, e: BaseException, error_msg: str) -> None:
        pass

    @dispatch(str)
    def user_warning(self, error_msg: str) -> None:
        pass

    def dev_error(self, e: BaseException, error_msg: str) -> None:
        pass

    def dev_fatal(self, e: BaseException, error_msg: str) -> None:
        pass

    def dev_debug(self, msg: str) -> None:
        pass


class LogItemGeneral(LogItemInterface):
    def __init__(self, log_container: LoggersContainer):
        self.__log_container = log_container

    @dispatch(str)
    def user_error(self, error_msg: str) -> None:
        raise NotImplementedError()

    @dispatch(BaseException, str)
    def user_error(self, e: BaseException, error_msg: str) -> None:
        raise NotImplementedError()

    @dispatch(str)
    def user_warning(self, error_msg: str) -> None:
        raise NotImplementedError()

    @dispatch(BaseException, str)
    def user_warning(self, e: BaseException, error_msg: str) -> None:
        raise NotImplementedError()

    def dev_error(self, e: BaseException, error_msg: str) -> None:
        self.__log_container.dev.error(
            LogFormatter.format_except(
                e,
                error_msg))

    def dev_fatal(self, e: BaseException, error_msg: str) -> None:
        self.__log_container.dev.fatal(
            LogFormatter.format_except(
                e,
                error_msg))

    def dev_debug(self, msg: str) -> None:
        self.__log_container.dev.debug(msg)


class LogItemForRequest(LogItemInterface):
    def __init__(self, log_container: LoggersContainer, req_id: str):
        self.__log_container = log_container
        self.__req_id = req_id

    @dispatch(str)
    def user_error(self, error_msg: str) -> None:
        msg = LogFormatter.format_req(self.__req_id, error_msg)
        self.__log_container.cli.error(msg)

    @dispatch(BaseException, str)
    def user_error(self, e: BaseException, error_msg: str) -> None:
        msg = LogFormatter.format_req(self.__req_id, e, error_msg)
        self.__log_container.cli.error(msg)

    @dispatch(str)
    def user_warning(self, error_msg: str) -> None:
        msg = LogFormatter.format_req(self.__req_id, error_msg)
        self.__log_container.cli.warning(msg)

    @dispatch(BaseException, str)
    def user_warning(self, e: BaseException, error_msg: str) -> None:
        msg = LogFormatter.format_req(self.__req_id, e, error_msg)
        self.__log_container.cli.warning(msg)

    def dev_error(self, e: BaseException, error_msg: str) -> None:
        msg = LogFormatter.format_req(self.__req_id, e, error_msg)
        self.__log_container.dev.error(msg)

    def dev_fatal(self, e: BaseException, error_msg: str) -> None:
        msg = LogFormatter.format_req(self.__req_id, e, error_msg)
        self.__log_container.dev.fatal(msg)

    def dev_debug(self, msg: str) -> None:
        msg = LogFormatter.format_req(self.__req_id, msg)
        self.__log_container.dev.debug(msg)
