import datetime
import os


class StringValidatorInterface:
    def validate(self, value_str: str):
        pass

    @property
    def format(self) -> str:
        pass


class BasicDateValidator(StringValidatorInterface):
    def __init__(self):
        self.__format = "%Y-%m-%d"

    @property
    def format(self) -> str:
        return self.__format

    def validate(self, value_str: str) -> bool:
        try:
            datetime.datetime.strptime(value_str, self.__format)
            return True
        except ValueError:
            return False


class RelativeFilePathValidator(StringValidatorInterface):
    def __init__(self):
        pass

    @property
    def format(self) -> str:
        return "relative, not contains '..'"

    def validate(self, value_str: str) -> bool:
        if len(value_str) == 0:
            return False
        absolute_removed = value_str[1:] if value_str[0] in ["/", "\\"] else value_str
        normalized = os.path.normcase(absolute_removed)
        is_not_abs = not os.path.isabs(normalized)
        folders = normalized.split(os.sep)
        has_no_parents = ".." not in folders
        return is_not_abs and has_no_parents

class RelativeRawFilePathValidator(StringValidatorInterface):
    def __init__(self):
        pass

    @property
    def format(self) -> str:
        return "relative, not contains '..'"

    def validate(self, value_str: str) -> bool:
        if len(value_str) == 0:
            return False
        absolute_removed = value_str[1:] if value_str[0] in ["/", "\\"] else value_str
        normalized = os.path.normcase(absolute_removed)
        is_not_abs = not os.path.isabs(normalized)
        folders = normalized.split(os.sep)
        has_no_parents = ".." not in folders
        return is_not_abs and has_no_parents