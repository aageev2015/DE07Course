from http import HTTPStatus
import requests
from typing import List, Dict, Any

from load_sales_raw_job1.dal.exceptions import *
from load_sales_raw_job1.dal.sales_api import SalesDalInterface
from logs_handling.log_formatter import LogFormatter
from logs_handling.log_item import LogItemInterface


class SalesDalCourseFake(SalesDalInterface):
    def __init__(self, api_url: str, token: str):
        super(SalesDalCourseFake, self).__init__()
        self.__api_url = api_url
        self.__token = token

    def get_sales(self, log: LogItemInterface, date_str: str, page_num: int) -> List[Dict[str, Any]]:
        """
        Get data from sales WebAPI for specified date and page.

        :param log: log-item for handling logs
        :param date_str: date filter
        :param page_num: page number filter
        :return: list of records
        """
        log.dev_debug(f"Request to CourseFake API on date {date_str}, page {page_num}")

        response = requests.get(
            url=self.__api_url,
            params={'date': date_str, 'page': page_num},
            headers={'Authorization': self.__token},
        )
        log.dev_debug(f"Response from CourseFake WebAPI. Status {response.status_code}. "
                      f"{len(response.content)} bytes was received")

        match response.status_code:
            case HTTPStatus.OK:
                try:
                    result: List[Dict[str, Any]] = response.json()
                except BaseException as e:
                    raise SalesDalAPIJSONFormatException(
                        LogFormatter.format_except(e, "CourseFake API. JSON in response wrong."))
            case HTTPStatus.NOT_FOUND | HTTPStatus.NO_CONTENT:
                result: List[Dict[str, Any]] = list[Dict[str, Any]]()
            case HTTPStatus.REQUEST_TIMEOUT | HTTPStatus.TOO_MANY_REQUESTS | HTTPStatus.INTERNAL_SERVER_ERROR | \
                    HTTPStatus.SERVICE_UNAVAILABLE | HTTPStatus.GATEWAY_TIMEOUT:
                raise SalesDalAPIRequestFailedTemporaryException(f"Response code response {response.status_code}")
            case _:
                raise SalesDalAPIRequestFailedException(f"Response code response {response.status_code}")

        return result
