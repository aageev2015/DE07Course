import os
import signal
import atexit
from pathlib import Path
from http import HTTPStatus

from flask import Flask, request, render_template
from flask import typing as flask_typing

from load_sales_raw_job1.bll.exceptions import *
from load_sales_raw_job1.bll.sales_api import SalesApiBlInterface
from load_sales_raw_job1.bll.sales_api_course_fake import SalesApiBllCourseFake
from load_sales_raw_job1.dal.exceptions import *
from load_sales_raw_job1.dal.sales_api import SalesDalInterface
from load_sales_raw_job1.dal.sales_api_course_fake import SalesDalCourseFake
from load_sales_raw_job1.dal.storage_api import StorageDalInterface
from load_sales_raw_job1.dal.storage_api_to_disk import StorageDalDisk

from logs_handling.log_item import LogItemInterface
from logs_handling.log_item_factory import LogItemFactory
from logs_handling.loggers_container import LoggersContainer
from support_tools.req_id_generator import ReqIdGeneratorRandom, ReqIdGeneratorInterface
from support_tools.file_tools import guarantee_folder_exists



# -- Default by source configs begin ---
WORKING_DIR = os.getcwd()
LOGS_PATH = os.environ.get("LOGS_PATH") or os.path.join(WORKING_DIR, 'logs')

LOAD_RAW_JOB_PORT = os.environ.get("LOAD_RAW_JOB_PORT") or 8081
RAW_LOADED_PATH = os.environ.get("RAW_LOADED_PATH") or os.path.join(WORKING_DIR, 'file_storage')
RAW_SALES_API_URL = os.environ.get("RAW_SALES_API_URL") or 'https://fake-api-vycpfa6oca-uc.a.run.app/sales'
RAW_LOAD_CONTINUE_ON_PAGE_FAIL = (os.environ.get("RAW_LOAD_CONTINUE_ON_PAGE_FAIL") == 1) or True
RAW_SALES_API_REQUESTS_DELAY_SEC = 0.2
RAW_SALES_API_MAX_FAILS = 3
RAW_FILE_NAME_TEMPLATE = "sales_%date%_%page%.json"
# --- Default by source  configs end ---


# -- Environment configs begin
RAW_LOAD_API_AUTH_TOKEN = os.environ.get("RAW_LOAD_API_AUTH_TOKEN")
# --- Environment configs end ---

print('Initialize...')
# --- Configs checks begin ---
if not LOAD_RAW_JOB_PORT:
    print("LOAD_RAW_JOB_PORT environment variable must be set")
if not RAW_LOADED_PATH:
    print("RAW_LOADED_PATH environment variable must be set")
if not RAW_SALES_API_URL:
    print("SALES_API_URL environment variable must be set")
if not RAW_LOAD_API_AUTH_TOKEN:
    print("AUTH_TOKEN environment variable must be set")
# --- Configs checks end ---

# -- Create global container begin ---

guarantee_folder_exists(RAW_LOADED_PATH)
guarantee_folder_exists(LOGS_PATH)

app: Flask = Flask(__name__)
loggers = LoggersContainer(str(Path(__file__).parent))
req_id_generator: ReqIdGeneratorInterface = ReqIdGeneratorRandom()
log_item_factory: LogItemFactory = LogItemFactory(loggers, req_id_generator)
log_item_gen: LogItemInterface = log_item_factory.new_general()

storage_dal: StorageDalInterface = StorageDalDisk(RAW_LOADED_PATH)
sales_dal: SalesDalInterface = SalesDalCourseFake(RAW_SALES_API_URL, RAW_LOAD_API_AUTH_TOKEN)
sales_bll: SalesApiBlInterface = SalesApiBllCourseFake(
    sales_dal, storage_dal,
    RAW_LOAD_CONTINUE_ON_PAGE_FAIL, RAW_SALES_API_REQUESTS_DELAY_SEC, RAW_SALES_API_MAX_FAILS,
    RAW_FILE_NAME_TEMPLATE)

# -- Create global container end ---
print('Ready to accept requests')


# -- Controller methods implementation begin ---
@app.route('/', methods=['GET'])
def root_index():
    return render_template('index.html')


@app.route('/', methods=['POST'])
def controller_load_sales_raw() -> flask_typing.ResponseReturnValue:
    """
    Controller that accepts sales raw data and save locally

    Proposed POST body in JSON:
    {
      "date: "2022-12-09",
      "raw_dir": "/raw/sales/2022-12-09"
    }
    """

    log: LogItemInterface = log_item_factory.new_for_request()
    try:
        try:
            input_data: dict = request.json
        except BaseException as e:
            log.user_error(e, "Client pass wrong JSON format")
            return {
                "message": "Wrong JSON",
            }, HTTPStatus.BAD_REQUEST

        date_str = input_data.get('date')
        raw_dir = input_data.get('raw_dir')

        if not date_str:
            log.user_error('"date" parameter missed by client')
            return {
                "message": f'"date" parameter missed',
            }, HTTPStatus.BAD_REQUEST
        if not raw_dir:
            log.user_error('"raw_dir" parameter missed by client')
            return {
                "message": f'"raw_dir" parameter missed',
            }, HTTPStatus.BAD_REQUEST

        sales_bll.save_sales_to_storage(log, date_str=date_str, raw_dir=raw_dir)
        return {
            "message": "Data retrieved successfully from API",
        }, HTTPStatus.CREATED

    except SalesBllDateFormatException as e:
        log.user_error(e, "Client pass date with wrong format")
        return {
            "message": str(e),
        }, HTTPStatus.BAD_REQUEST
    except SalesBllPathFormatException as e:
        log.user_error(e, "Client pass not acceptable logical file path")
        return {
            "message": str(e),
        }, HTTPStatus.BAD_REQUEST
    except SalesBllNothingLoadedException as e:
        log.user_warning(e, "Sales not found")
        return {
            "message": f"Sales not found. {e}",
        }, HTTPStatus.NOT_FOUND
    except SalesBllPartiallyLoadedException as e:
        log.dev_error(e, f"Sales loaded partially. {e}")
        return {
            "message": f"Sales loaded partially. {e}",
        }, HTTPStatus.PARTIAL_CONTENT
    except SalesBllAllTriesFailedException as e:
        log.dev_error(e, "All tries failed")
        return {
            "message": "Internal API error",
        }, HTTPStatus.INTERNAL_SERVER_ERROR
    except SalesDalAPIJSONFormatException as e:
        log.dev_error(e, "Vendor's sales API return wrong JSON format")
        return {
            "message": "Internal API error",
        }, HTTPStatus.INTERNAL_SERVER_ERROR
    except SalesDalAPIRequestFailedException as e:
        log.dev_error(e, "Vendor's sales API request failed")
        return {
            "message": "Internal API error",
        }, HTTPStatus.INTERNAL_SERVER_ERROR
    except SalesDalStorageSaveException as e:
        log.dev_error(e, "Saving to local disk failed")
        return {
            "message": "Internal API error",
        }, HTTPStatus.INTERNAL_SERVER_ERROR
    except BaseException as e:
        log.dev_fatal(e, 'Unexpected')
        return {
            "message": "Internal API error",
        }, HTTPStatus.INTERNAL_SERVER_ERROR


# -- Controller methods implementation end ---


# -- Termination handling begin --
def handle_exit(reason: str):
    try:
        loggers.terminate(reason)
    except BaseException as e:
        msg = 'Termination failed\n' \
              f'{str(e)}'
        print(msg)
        loggers.dev.fatal(msg)


atexit.register(lambda *args: handle_exit('atexit invoked'))
signal.signal(signal.SIGTERM, lambda *args: handle_exit('signal SIGTERM invoked'))
signal.signal(signal.SIGINT, lambda *args: handle_exit('signal SIGINT invoked'))
# -- Termination handling end --

if __name__ == '__main__':
    app.run(debug=True, host="localhost", port=LOAD_RAW_JOB_PORT)
