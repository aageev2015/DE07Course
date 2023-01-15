from multipledispatch import dispatch


class LogFormatter:
    @staticmethod
    @dispatch(str, str)
    def format_req(req_id: str, msg: str) -> str:
        return f'req_id: {req_id}\n{msg}'

    @staticmethod
    @dispatch(str, BaseException, str)
    def format_req(req_id: str, e: BaseException, msg: str) -> str:
        return f'req_id: {req_id}. {type(e).__name__}\n{msg}\n{e}'

    @staticmethod
    @dispatch(BaseException, str)
    def format_except(e: BaseException, msg: str) -> str:
        return f'{type(e).__name__}\n{msg}\n{e}'
