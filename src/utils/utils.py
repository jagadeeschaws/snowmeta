"""
Data process utils component
"""
import os
import argparse
import json
import sys
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake.snowpark import Session
from src.utils.logr import fw_logger, sf_logger


def read_s3_file(file_name):
    """
    Reads configuration file from s3 file.

    Parameters:
    -file_name (str): The name of the configuration file.

    """
    s3 = boto3.client("s3")
    bucket, key = file_name.replace("s3://", "").split("/", 1)
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        return response["Body"].read()
    except ClientError as e:
        raise ValueError(
            f"Unexpected error occurred while loading config from S3: {e}"
        ) from e
    except Exception as e:
        raise ValueError(f"Unexpected error occurred while loading config: {e}") from e


def get_config(file_name: str) -> dict:
    """
    Reads and returns the content of a JSON configuration file.

    Parameters:
    - file_name (str): The name of the JSON configuration file.

    Returns:
    - dict: The content of the JSON configuration file.
    """
    if file_name.startswith("s3://"):
        s3_out = read_s3_file(file_name)
        return json.loads(s3_out.decode("utf-8"))

    try:
        with open(file_name, encoding="utf-8") as config_file:
            return json.load(config_file)
    except FileNotFoundError as fnf:
        raise FileNotFoundError(f"Configuration file '{file_name}' not found") from fnf
    except Exception as e:
        raise ValueError(f"Unexpected error occurred while loading config: {e}") from e


def get_private_key(pass_phrase: str, private_key_file: str):
    """
    Load the private key from the specified file and return its content as DER-encoded bytes.

    Args:
    - pass_phrase (str): The passphrase used to decrypt the private key file, if it is encrypted.
    - private_key_file (str): The path or S3 URL of the file containing the private key.

    Returns:
    - bytes: The DER-encoded content of the private key file.

    """
    sf_logger.info("Reading private key file")
    if private_key_file.startswith("s3://"):
        # Handle private key file in S3
        key_bytes = read_s3_file(private_key_file)
    else:
        # Handle private key file on local disk
        with open(private_key_file, "rb") as key:
            key_bytes = key.read()

    p_key = serialization.load_pem_private_key(
        key_bytes, password=pass_phrase.encode(), backend=default_backend()
    )

    if p_key is None:
        raise Exception("Failed to load the private key")

    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def create_session(conn_prop: dict) -> Session:
    """
    Creates a Snowpark session with configurations specified in `conn_prop`

    Parameters:
    conn_prop (dict): A dictionary of snowflake configurations

    Returns:
    Session: A Snowpark session
    """
    sf_logger.info("Creating a Snowpark Session")
    try:
        if not conn_prop.get("warehouse"):
            raise ValueError("`warehouse` must be specified in the config file")

        if not conn_prop.get("role"):
            raise ValueError("`role` must be specified in the config file")

        if not conn_prop.get("database"):
            raise ValueError("`database` must be specified in the config file")

        if not conn_prop.get("schema"):
            raise ValueError("`schema` must be specified in the config file")

        # pass_phrase = os.environ.get("pass_phrase") or conn_prop.get("pass_phrase")
        #
        # if not pass_phrase:
        #     raise ValueError("`pass_phrase` is empty")
        #
        # if not conn_prop.get("private_key_file"):
        #     raise ValueError("`private_key_file` must be provided")
        #
        # conn_prop["private_key"] = get_private_key(
        #     pass_phrase, conn_prop["private_key_file"]
        # )

        ss = Session.builder.configs(conn_prop).create()
    except Exception as e:
        raise ConnectionError(f"Creating Snowpark session failed with {e}") from e
    return ss


def log_str_size_check(log_str: str) -> str:
    """
    Truncates a given string to 16 MB if it exceeds that size.

    Args:
        log_str (str): The string to check.

    Returns:
        str: The truncated string.
    """
    MAX_LOG_SIZE = 16 * 1024 * 1024  # 16 MB in bytes

    if sys.getsizeof(log_str) > MAX_LOG_SIZE:
        return log_str[:MAX_LOG_SIZE]

    return log_str


def update_job_status(**params):
    """
    Updates the job status into job status history table.

    params (dict): A dictionary containing the parameters
    - ss (Session): The Snowpark session object.
    - process_name (str): The name of the process.
    - action (str): The action to be taken, either 'insert' or 'update'.
    - resolved_pipeline (str): The resolved pipeline as a JSON string.
    - run_id (str): The run ID of the process.
    - restart_run_id (str): The restart run ID of the process.
    - database (str): The name of the database.
    - schema (str): The name of the schema.
    - pipeline_status (str): The status of the pipeline.
    - log_msg_str (str): The log message of the process.
    - ss_wh (str): The compute warehouse.
    """
    process_name = params.get("process_name")
    action = params.get("action")
    pipeline_config = params.get("pipeline_config")
    run_id = params.get("run_id")
    restart_run_id = params.get("restart_run_id")
    pipeline_status = params.get("pipeline_status")
    log_msg_str = params.get("log_msg_str")
    meta_wh = params.get("meta_wh")
    ss = params.get("ss")
    successful_record_count = params.get("successful_record_count", 0)
    failed_record_count = params.get("failed_record_count", 0)
    failure_reason = params.get("failure_reason", "null")

    if action == "insert":
        fw_logger.debug(
            f"Inserting status into {params['database']}.{params['schema']}.job_status_history"
        )

        restart_run_id = restart_run_id if restart_run_id else "NULL"

        ss.sql(
            f"insert into {params['database']}.{params['schema']}.job_status_history "
            f"(PROCESS_NAME, PIPELINE_CONFIG, RUN_ID, RESTART_RUN_ID, START_DATE_TIME, END_DATE_TIME, "
            f"LOG_MESSAGE, STATUS, SUCCESSFUL_RECORD_COUNT, FAILED_RECORD_COUNT, FAILURE_REASON, CREATE_USER, "
            f"UPDATE_USER)"
            f"select '{process_name}', "
            f"parse_json($${pipeline_config}$$),"
            f"{run_id}, "
            f"{restart_run_id}, "
            f"current_timestamp(),"
            f"null,"
            f"null,"
            f"'STARTED',"
            f"null,"
            f"null,"
            f"null,"
            f"current_user(),"
            f"current_user()"
        ).show()
    elif action == "update":
        fw_logger.debug(
            f"Updating status into {params['database']}.{params['schema']}.job_status_history"
        )

        ss.sql(f"USE WAREHOUSE {meta_wh}").collect()
        log_msg = log_msg_str.replace("'", "''").replace(r"\u", r"\\u")

        ss.sql(
            f"update {params['database']}.{params['schema']}.job_status_history "
            f"set end_date_time = current_timestamp(),"
            f"status = '{pipeline_status}',"
            f"log_message = '{log_msg}',"
            f"successful_record_count = {successful_record_count},"
            f"failed_record_count = {failed_record_count},"
            f"failure_reason = '{failure_reason}' "
            f"where run_id = {run_id} "
            f"and process_name = '{process_name}'"
        ).show()

    else:
        pass


def print_stat_send_email(ss: Session, func_args) -> None:
    """
    Print stat and sends an email using the specified email integration and email address list.

    Args:
        ss (Session): The Snowpark session object.
        func_args (dict): A dictionary containing the following keys:
            - 'email_intg_name' (str): The name of the email integration.
            - 'email_addr_list' (list): A list of email addresses to send the email to.
            - 'status' (str): The status of the pipeline job ('SUCCEEDED' or 'FAILED').
            - 'process_name' (str): The name of the pipeline job.
            - 'run_id' (str): The ID of the pipeline job run.
            - 'total_records_processed' (int): The total number of records processed by the pipeline job.
            - 'failed_record_count' (int): The number of records that failed to process, if applicable.
            - 'failure_reason' : The reason for failure

    Returns:
        None
    """

    enable_send_email = False
    if func_args["status"].upper() == "SUCCEEDED":

        pipeline_stat = f"""

                            Start Time: {func_args["start_time"]}
                            End Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
                            Run ID: {func_args["run_id"]}
                            Total records processed: {func_args["total_records_processed"]}
                            Failed record count: {func_args["failed_record_count"]}

                            """

        if func_args["email_notification_subscription"] in ["success", "both"]:
            enable_send_email = True
            email_subject = (
                f"Process {func_args['process_name']} has been completed successfully"
            )
            pipeline_status_msg = (
                f'The pipeline `{func_args["process_name"]}` has completed successfully.'
            )

    else:
        if func_args["failure_reason"]:
            pipeline_stat = f"""
                                Start Time: {func_args["start_time"]}
                                End Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
                                Run ID: {func_args["run_id"]}
                                Total records processed: {func_args["total_records_processed"]}
                                Failed record count: {func_args["failed_record_count"]}
                                failure reason: {func_args["failure_reason"]}
                                """
        else:
            pipeline_stat = f"""
                                Please check the `job_status_history` table for more details.   
                                Start Time: {func_args['start_time']}
                                End Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
                                Run ID: {func_args['run_id']}
                                """
        if func_args["email_notification_subscription"] in ["failure", "both"]:
            enable_send_email = True
            email_subject = f"Process {func_args['process_name']} has been failed"
            pipeline_status_msg = f'The pipeline `{func_args["process_name"]}` has failed.'

    fw_logger.info(pipeline_stat)

    # Send email notification
    if func_args["email_intg_name"] and enable_send_email:
        fw_logger.info("Sending email to configured email_list")
        email_list = ",".join(func_args["email_addr_list"])
        email_body = f""" Hello
            {pipeline_status_msg} 
            {pipeline_stat}
            Thank you."""
        send_email_expression = (
            f"call system$send_email("
            f"'{func_args['email_intg_name']}',"
            f"'{email_list}',"
            f"'{email_subject}',"
            f"'{email_body}')"
        )
        try:
            ss.sql(send_email_expression).collect()
        except Exception as e:
            sf_logger.warning(
                f"Failure in sending email notification {e}. "
                f"Please check status in `job_status_history` table for run_id : `{func_args['run_id']}` "
                f"for more details"
            )


def get_args():
    """
    Parses command line arguments to get the pipeline and runtime configuration.

    Returns:
    Namespace: A namespace object containing the following attributes:
    process_name (str): The name of the process to run.
    restart_run_id (str, optional): The run ID to restart from. If not provided, the pipeline starts from the
            beginning.
    """
    # Parse command line arguments to get pipeline and runtime config
    parser = argparse.ArgumentParser()

    # Required Arguments
    parser.add_argument("--process_name", type=str, required=True)
    parser.add_argument("--config_file", type=str, required=True)
    parser.add_argument("--restart_step_id", type=int, required=False, default="000")
    parser.add_argument("--restart_run_id", type=int, required=False)
    parser.add_argument("--status_code", type=str, required=False)
    parser.add_argument(
        "--log_level", default="INFO", help="Log level (default: %(default)s)"
    )
    parser.add_argument("--runtime_params", type=str, required=False)

    return parser.parse_args()
