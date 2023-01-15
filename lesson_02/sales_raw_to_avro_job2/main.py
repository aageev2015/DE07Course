import os
import re
import signal
import atexit
from pathlib import Path
from http import HTTPStatus

from flask import Flask, request, render_template
from flask import typing as flask_typing

from sales_raw_to_avro_job2.bll.exceptions import *
from sales_raw_to_avro_job2.bll.sales_api import SalesApiBlInterface
from sales_raw_to_avro_job2.bll.sales_api_course_fake import SalesApiBllCourseFake
from sales_raw_to_avro_job2.dal.exceptions import *
from sales_raw_to_avro_job2.dal.storage_api import StorageDalInterface
from sales_raw_to_avro_job2.dal.storage_api_to_disk import StorageDalDisk

from logs_handling.log_item import LogItemInterface
from logs_handling.log_item_factory import LogItemFactory
from logs_handling.loggers_container import LoggersContainer
from support_tools.req_id_generator import ReqIdGeneratorRandom, ReqIdGeneratorInterface
from support_tools.file_tools import guarantee_folder_exists

# -- Default by source configs begin ---
WORKING_DIR = os.getcwd()
LOGS_PATH = os.environ.get("LOGS_PATH") or os.path.join(WORKING_DIR, 'logs')

RAW_TO_AVRO_JOB_PORT = os.environ.get("RAW_TO_AVRO_JOB_PORT") or 8082
STG_SAVED_PATH = os.environ.get("STG_SAVED_PATH") or os.path.join(WORKING_DIR, 'file_storage')
RAW_PATH = os.environ.get("RAW_PATH") or os.path.join(WORKING_DIR, 'file_storage')
STG_FILE_NAME_REGEX = r'\1.avro'
RAW_FILE_NAME_REGEX = r'^(sales_\d\d\d\d-\d\d-\d\d_\d+).json$'
AVRO_SCHEMA_CONFIG_FILE = os.path.join(Path(__file__).parent, 'sales_stg.avsc')

# --- Default by source  configs end ---

print('Initialize...')
# --- Configs checks begin ---
if not RAW_TO_AVRO_JOB_PORT:
    print("RAW_TO_AVRO_JOB_PORT environment variable must be set")
if not STG_SAVED_PATH:
    print("STG_SAVED_PATH environment variable must be set")
if not RAW_PATH:
    print("RAW_PATH environment variable must be set")
if re.compile(RAW_FILE_NAME_REGEX).sub(STG_FILE_NAME_REGEX, "sales_2000-01-01_1.json") == "sales_2000-01-01_1.json":
    print("RAW_FILE_NAME_REGEX must convert to STG_FILE_NAME_REGEX into different file name")
# --- Configs checks end ---

# -- Create global container begin ---

# noinspection DuplicatedCode
guarantee_folder_exists(STG_SAVED_PATH)
guarantee_folder_exists(LOGS_PATH)

app: Flask = Flask(__name__)
loggers = LoggersContainer(str(Path(__file__).parent))
req_id_generator: ReqIdGeneratorInterface = ReqIdGeneratorRandom()
log_item_factory: LogItemFactory = LogItemFactory(loggers, req_id_generator)
log_item_gen: LogItemInterface = log_item_factory.new_general()

storage_dal: StorageDalInterface = StorageDalDisk(STG_SAVED_PATH, RAW_PATH, AVRO_SCHEMA_CONFIG_FILE)
sales_bll: SalesApiBlInterface = SalesApiBllCourseFake(
    storage_dal,
    STG_FILE_NAME_REGEX, RAW_FILE_NAME_REGEX)

# -- Create global container end ---
print('Ready to accept requests')


# -- Controller methods implementation begin ---
@app.route('/', methods=['GET'])
def root_index():
    return render_template('index.html')


@app.route('/', methods=['POST'])
def controller_sales_raw_to_avro() -> flask_typing.ResponseReturnValue:
    """
    Controller that convert raw data to avro format

    Proposed POST body in JSON:
    {
      "stg_dir": "/stg/sales/2022-12-09"
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

        not_supported_parameters: set = set(input_data.keys()).difference(['stg_dir', 'raw_dir'])
        if len(not_supported_parameters) > 0:
            return {
                "message": f'Parameters not supported: {not_supported_parameters}',
            }, HTTPStatus.IM_A_TEAPOT

        stg_dir = input_data.get('stg_dir')
        raw_dir = input_data.get('raw_dir')

        if not stg_dir:
            log.user_error('"stg_dir" parameter missed by client')
            return {
                "message": f'"stg_dir" parameter missed',
            }, HTTPStatus.BAD_REQUEST
        if not raw_dir:
            log.user_error('"raw_dir" parameter missed by client')
            return {
                "message": f'"raw_dir" parameter missed',
            }, HTTPStatus.BAD_REQUEST

        sales_bll.convert_raw_to_avro(log, stg_dir=stg_dir, raw_dir=raw_dir)
        return {
            "message": "Data converted successfully",
        }, HTTPStatus.CREATED

    except SalesBllPathFormatException as e:
        log.user_error(e, "Client pass not acceptable logical file path")
        return {
            "message": str(e),
        }, HTTPStatus.BAD_REQUEST
    except SalesBllRawFolderNotFoundOrEmptyException as e:
        log.user_warning(e, "Raw folder not found or empty")
        return {
            "message": f"Raw folder not found or empty",
        }, HTTPStatus.NOT_FOUND
    except SalesDalStorageLoadRawException as e:
        log.dev_error(e, "Loading from local disk failed")
        return {
            "message": "Internal API error",
        }, HTTPStatus.INTERNAL_SERVER_ERROR
    except SalesDalStorageSaveAvroException as e:
        log.dev_error(e, "Saving to local disk failed")
        return {
            "message": "Internal API error",
        }, HTTPStatus.INTERNAL_SERVER_ERROR
    except SalesDalAvroDateFormatException as e:
        log.dev_error(e, "Wrong date for avro")
        return {
            "message": str(e),
        }, HTTPStatus.UNPROCESSABLE_ENTITY
    except SalesDalAvroKeyErrorException as e:
        log.dev_error(e, "Key not found for avro")
        return {
            "message": str(e),
        }, HTTPStatus.UNPROCESSABLE_ENTITY
    except BaseException as e:
        log.dev_fatal(e, 'Unexpected')
        return {
            "message": "Internal API error",
        }, HTTPStatus.INTERNAL_SERVER_ERROR


# -- Controller methods implementation end ---


# -- Termination handling begin --
# noinspection DuplicatedCode
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
    app.run(debug=True, host="localhost", port=RAW_TO_AVRO_JOB_PORT)
