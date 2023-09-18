"""mparticle functions"""

import os
import json
import time
import random
import queue
import traceback
import concurrent.futures
from uuid import uuid4
from datetime import datetime
import mparticle
import pandas as pd
from urllib3.exceptions import ProtocolError
from snowflake.snowpark import Session, dataframe

from src.data_processor.core.mp_defaults import MP_DEFAULT_VALUES
from src.utils.logr import sf_logger


def send_df_to_mp(mp_conf, source_df_xform, data_plan_event, func_args, payload_conf):
    """
    Iterate through source dataframe and construct payload to send to mparticle.
    Args:
        - mp_conf (dict): A dictionary containing mparticle configuration.
        - source_df_xform (dataframe): Dataframe of source data.
        - data_plan_event (dict): A dictionary of data plan and event information.
        - func_args (dict): A dictionary of arguments that contains configuration information for the function.
        - payload_conf (dict): A dictionary containing information required for payload

    Returns:
        dict: A dictionary of results.

    """
    # Initialize record count
    record_count = 0

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=mp_conf.get("max_workers", MP_DEFAULT_VALUES["THREADS"])
    ) as executor:
        futures = []
        payload_list = []
        try:
            for batch_source_df in source_df_xform.to_pandas_batches():
                sf_logger.info(f"batch size: {len(batch_source_df)}")
                batch_source_df = batch_source_df.replace({pd.NaT: None})
                for rec in batch_source_df.to_records(index=False):
                    record_count += 1
                    payload_list.append(build_pay_load(
                        mp_conf.get("environment"),
                        data_plan_event,
                        func_args["column_mapping"],
                        rec,
                        mp_conf["restart_from_mp_sink"],
                    ))
                    if len(payload_list) == 100:
                        if record_count == 1:  # first record to handle auth exception
                            try:
                                send_data(payload_list, payload_conf)
                            except Exception as e:
                                raise Exception(e) from e
                        else:
                            futures.append(
                                executor.submit(send_data, payload_list, payload_conf)
                            )
                            payload_list = []
                    else:
                        pass

            if len(payload_list) > 0:
                futures.append(
                    executor.submit(send_data, payload_list, payload_conf)
                )

            for future in concurrent.futures.as_completed(futures):
                if future.exception() is not None:
                    sf_logger.error("mparticle sink function has been failed")

                    executor.shutdown(wait=False)

                    traceback.print_exc()

                    failed_rec_count = get_total_failed_rec_count(
                        count_queue=payload_conf["count_queue"]
                    )

                    sf_logger.info(
                        f"Total processed records {record_count} and "
                        f"failed record count: {failed_rec_count}"
                    )
                    raise Exception(
                        f"Failed to send data to mParticle with exception: {future.exception()}"
                    )

            executor.shutdown(wait=False)

            flush_failed_records(
                payload_conf["ss"],
                payload_conf["failed_table_name"],
                payload_conf["failed_records"],
                payload_conf["count_queue"],
                payload_conf["lock"],
            )

            failed_rec_count = get_total_failed_rec_count(
                count_queue=payload_conf["count_queue"]
            )

            sf_logger.info(
                f"Successful completion of the mparticle sink function. "
                f"Total records processed: {record_count} and "
                f"failed record count: {failed_rec_count}"
            )
        except Exception as e:
            traceback.print_exc()
            raise Exception(
                f"Data processing failed while sending to "
                f"mparticle with following {e}"
            ) from e

    return {
        "threshold_percentage": func_args["mp_properties"].get(
            "THRESHOLD_PERCENTAGE",
            func_args["mp_properties"].get("threshold_percentage"),
        ),
        "total_records_processed": record_count,
        "failed_record_count": failed_rec_count,
    }


def get_data_plan_event(mp_prop):
    """
    Extracts the data plan and event information from the given `mp_prop` dictionary.
    Args:
        mp_prop (dict): A dictionary containing the following keys:
            - plan_id (str): The ID of the data plan (can be in uppercase or lowercase).
            - plan_version (str): The version of the data plan (can be in uppercase or lowercase).
            - event_name (str): The name of event (can be in uppercase or lowercase).
            - event_type (str): The type of event (can be in uppercase or lowercase).

    Returns:
        dict: A dictionary containing the `plan_id` and `plan_version` properties extracted from `mp_prop`.

    """
    plan_id = mp_prop.get("plan_id") or mp_prop.get("PLAN_ID")
    plan_version = mp_prop.get("plan_version") or mp_prop.get("PLAN_VERSION")

    if not plan_id:
        raise ValueError("plan_id not found in mp_properties")

    if plan_version:
        data_plan = {"data_plan": {"plan_id": plan_id, "plan_version": plan_version}}

    else:
        data_plan = {"data_plan": {"plan_id": plan_id}}

    event_name = mp_prop.get("event_name") or mp_prop.get("EVENT_NAME")
    event_type = mp_prop.get("event_type") or mp_prop.get("EVENT_TYPE")

    if not event_type:
        event_config = {"event_config": {"event_name": event_name}}
    else:
        event_config = {
            "event_config": {"event_name": event_name, "custom_event_type": event_type}
        }
    return {**data_plan, **event_config}


def create_mp_configuration(mp_conf):
    """
    Creates and returns a Configuration object for the mParticle API.

    Args:
        mp_conf (dict): A dictionary containing the following keys:
            - api_key (str): The API key for the mParticle API.
            - api_secret (str): The API secret for the mParticle API.
            - connection_pool_size (str): The maximum number of connections
                                        allowed in the connection pool (default: '1').
            - debug_enabled (str): Whether debug mode is enabled (default: 'false').

    Returns:
        Configuration: A Configuration object with the specified settings.
    """
    # Create a Configuration object
    configuration = mparticle.Configuration()

    # configuration.api_key = mp_conf.get('api_key', '')
    # configuration.api_secret = mp_conf.get('api_secret', '')
    configuration.api_key = os.environ.get("MP_API_KEY") or mp_conf.get(
        "MP_API_KEY", ""
    )
    configuration.api_secret = os.environ.get("MP_API_SECRET") or mp_conf.get(
        "MP_API_SECRET", ""
    )
    configuration.connection_pool_size = int(
        mp_conf.get("connection_pool_size", MP_DEFAULT_VALUES["CONNECTION_POOL_SIZE"])
    )
    configuration.debug = mp_conf.get("debug_enabled", "false").lower() == "true"
    return configuration


def get_source_df(ss: Session, df_dict: dict, func_args: dict, failed_rec_table):
    """Check restart flag and get source data"""
    if df_dict.get("restart_flag") and int(df_dict.get("restart_step_id")) == int(
        func_args["step_id"]
    ):
        restart_run_id = df_dict.get("restart_run_id")

        # construct filter condition
        if df_dict.get("status_code"):
            cond = f"RUN_ID = '{restart_run_id}' and STATUS_CODE in ({df_dict['status_code']})"
        else:
            cond = f"RUN_ID = '{restart_run_id}'"

        source_df_xform: dataframe = ss.table(failed_rec_table).where(cond)

        restart_from_mp_sink = True
    else:
        restart_from_mp_sink = False
        # Get the source dataframe
        source_df: dataframe = df_dict[func_args["input_df"]]
        # Apply transformations
        source_df_xform: dataframe = source_df.selectExpr(func_args["select_exp"])
    return source_df_xform, restart_from_mp_sink


def flush_failed_records(
    ss: Session,
    sf_table_name: str,
    failed_records: queue.Queue,
    count_queue: queue.Queue,
    lock,
):
    """
    Flushes the contents of the failed records queue to a snowflake table. The queue is consumed
    in batches of up to `batch_size` records, and each batch is inserted into the database
    as a separate transaction.

    This function should be called periodically to ensure that failed records are processed
    and stored in the database.

    Returns:
        None.
    """
    # acquire the lock before accessing the queue
    lock.acquire()
    failed_records_batch = []
    sf_logger.info("Consuming failed records queue")

    while not failed_records.empty():
        try:
            failed_records_batch.append(failed_records.get_nowait())
        except queue.Empty:
            break

    if failed_records_batch:
        failed_record_count = len(failed_records_batch)
        sf_logger.info(f"Flushed {failed_record_count} failed records from the queue")
        count_queue.put(failed_record_count)
        sf_logger.info("Inserting failed records into the Snowflake table")
        try:
            failed_records_df = ss.create_dataframe(
                failed_records_batch,
                [
                    "request_date",
                    "request_ts",
                    "run_id",
                    "status_code",
                    "failed_reason",
                    "source_request_id",
                    "data",
                ],
            )
            failed_records_df.write.mode("append").saveAsTable(
                sf_table_name
            )  # get this from configuration
            sf_logger.info(
                f"Inserted {failed_record_count} failed records "
                f"from batch {count_queue.qsize()} into the Snowflake table {sf_table_name}"
            )
        except Exception as fail_rec_e:
            sf_logger.error(
                f"Snowflake insert failed with {fail_rec_e} while writing failed records "
                f"into the Snowflake table {sf_table_name}"
            )
            raise ConnectionError(fail_rec_e) from fail_rec_e
        finally:
            failed_records_batch.clear()
            # release the lock after the queue has been flushed
            lock.release()


# Get the counts from the queue and calculate the final count
def get_total_failed_rec_count(count_queue: queue.Queue):
    """
    Retrieves the total number of failed records that have been processed by the `flush_failed_records` function.

    This function should be called at the end to get number of failed records that have been processed stored
    in the snowflake table.

    Returns:
        An integer representing the total number of failed records that have been processed.
    """
    sf_logger.info("Retrieving failed records queue")
    total_count = 0
    while not count_queue.empty():
        count = count_queue.get()
        total_count += count
    return total_count


def wait_before_retry(wait_tm):
    """
    This function takes in a wait time in seconds, adds a random delay to it,
    logs a warning message with the total wait time, and then waits for the total wait time before returning.

    Parameters:
    wait_tm (float): The wait time in seconds.

    Returns:
    None.
    """
    total_wait_time = wait_tm + random.random()
    sf_logger.warning(f"Retry after {round(total_wait_time, 2)} seconds.")
    time.sleep(total_wait_time)


def exception_mparticle(exception_params):
    """
    The function exception_mparticle handles exceptions raised by the send_data function
    when sending data to mParticle. It takes in a dictionary of exception_params as input
    and performs various actions based on the type of exception.

    Parameters:

    exception_params (dict): A dictionary of parameters containing the following keys:
        send_data_e (Exception): The exception that was raised by the send_data function.
        num_retries (int): The number of times the send_data function has retried sending the data.
        max_retries (int): The maximum number of times the send_data function can retry sending the data.
        add_failed_record (function): A function to add failed records to a table.
        wait_time (float): The initial wait time before retrying.
        max_wait_time (float): The maximum wait time between retries.
    Returns:
    None.

    The function raises an exception if the maximum number of retries is exceeded.
    """
    send_data_e = exception_params["send_data_e"]
    num_retries = exception_params["num_retries"]
    max_retries = exception_params["max_retries"]
    add_failed_record = exception_params["add_failed_record"]
    wait_time = exception_params["wait_time"]
    max_wait_time = exception_params["max_wait_time"]

    if send_data_e.status == 429:
        sf_logger.warning(f"mParticle API rate limit exceeded : {send_data_e.status}")

        if num_retries == max_retries + 1:
            add_failed_record(send_data_e.status, send_data_e.reason)
        else:
            sf_logger.warning(f"Retry attempt {num_retries} of {max_retries}")

            retry_after = int(send_data_e.headers.get("Retry-After", "0"))

            if retry_after > 0:
                wait_before_retry(retry_after)
            else:
                wait_before_retry(min(wait_time * 2, max_wait_time))

    elif send_data_e.status == 400 or send_data_e.status // 100 == 5:
        # Bad Request or Server error
        sf_logger.warning(f"Received a {send_data_e.status} error.")

        if num_retries == max_retries + 1:
            add_failed_record(send_data_e.status, send_data_e.reason)
        else:
            sf_logger.warning(f"Retry attempt {num_retries} of {max_retries}")

            wait_before_retry(min(wait_time * 2, max_wait_time))
    else:
        # Unhandled exception
        raise Exception(f"Received a {send_data_e} error") from send_data_e


def send_data(data, payload_conf, callback=None):
    """
    Sends data to an API using the given API client, with support for retrying failed requests
    and batching data into smaller chunks.

    Parameters:
    - payload (dict): The payload to send.
    - payload_conf: Additional payload configurations.
        - ss (Session): The Snowpark session object.
        - api_inst: The API instance object.
        - max_retries (int): The maximum number of retries.
        - base_wait_time (float): The base wait time between retries.
        - max_wait_time (float): The maximum wait time between retries.
        - run_id (int): The ID of the current run.
        - failed_table_name (str): The name of the table to store failed records.
        - failed_records (list): A list of failed records.
        - count_queue (queue.Queue): A queue to store counts.
        - lock (threading.Lock): A lock to synchronize count_queue.
        - batch_size (int): The batch size for sending data to the endpoint.
    - callback: An optional function to call after each successful request.

    Returns:
        None.
    """

    # Function to add failed records to the queue
    def add_failed_record(error_status, error_reason=None):
        failed_records.put(
            (
                datetime.now().strftime("%Y-%m-%d"),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                run_id,
                error_status,
                error_reason,
                data.user_identities.email,
                data.to_dict(),
            )
        )
        if failed_records.qsize() == batch_size:
            flush_failed_records(
                payload_conf.get("ss"),
                payload_conf.get("failed_table_name"),
                failed_records,
                payload_conf.get("count_queue"),
                payload_conf.get("lock"),
            )
        sf_logger.warning(
            "Maximum retries reached. Record added to failed records queue"
        )

    api_inst = payload_conf.get("api_inst")
    max_retries = payload_conf.get("max_retries")
    base_wait_time = payload_conf.get("base_wait_time")
    max_wait_time = payload_conf.get("max_wait_time")
    run_id = payload_conf.get("run_id")
    failed_records = payload_conf.get("failed_records")
    batch_size = payload_conf.get("batch_size")

    num_retries = 0
    wait_time = base_wait_time

    while num_retries <= max_retries:
        try:
            # response = api_inst.upload_events(data, callback=callback)
            response = api_inst.upload_events(data, callback=callback)

            if response[1] in (200, 202):       # status code
                num_retries = max_retries + 1   # Successful
            else:
                raise Exception(response)

        except ProtocolError as send_data_e:
            sf_logger.warning(f"Protocol error occurred: {send_data_e}.")

            num_retries += 1

            if num_retries == max_retries + 1:
                add_failed_record(999, "Protocol error")
            else:
                sf_logger.warning(f"Retry attempt {num_retries} of {max_retries}")

                wait_before_retry(min(wait_time * 2, max_wait_time))

        except AttributeError:

            add_failed_record(400, "Attribute error")
            num_retries = max_retries + 1

        except Exception as send_data_e:
            num_retries += 1
            exception_mparticle(
                {
                    "send_data_e": send_data_e,
                    "num_retries": num_retries,
                    "max_retries": max_retries,
                    "add_failed_record": add_failed_record,
                    "wait_time": wait_time,
                    "max_wait_time": max_wait_time,
                }
            )


# Build the payload for mParticle API
def build_pay_load(
    environment, data_plan_event, column_mapping, record, restart_mp_sink_flag
):
    """Build payload for mparticle"""
    if not restart_mp_sink_flag:
        mbatch = mparticle.Batch()
        mbatch.environment = environment

        mbatch.source_request_id = str(uuid4())

        mbatch.context = {"data_plan": data_plan_event.get("data_plan")}

        # Data be associated with a user identity
        if "user_identities" in column_mapping:
            # Build the user identities from the record and column mapping
            identities = {
                str(target_field).lower(): record[str(src_field).upper()]
                for src_field, target_field in column_mapping["user_identities"].items()
            }
            mbatch.user_identities = mparticle.UserIdentities(**identities)
            sf_logger.debug(f"User identities built: {mbatch.user_identities}")

        # User attributes
        # The mParticle audience platform can be powered by only sending a combination of user attributes,
        # used to describe segments of users, and device identities/user identities used to then target those users.

        if "user_attributes" in column_mapping:
            user_attributes = {}
            for source_fields, target_field in column_mapping[
                "user_attributes"
            ].items():
                if column_mapping.get("datatype_mapping"):
                    if (
                        column_mapping["datatype_mapping"].get(source_fields.upper())
                        == "dict"
                        or column_mapping["datatype_mapping"].get(source_fields.upper())
                        == "list"
                    ) and record[source_fields.upper()] is not None:
                        user_attributes[target_field] = json.loads(
                            record[source_fields.upper()]
                        )
                    else:
                        user_attributes[target_field] = record[source_fields.upper()]
                else:
                    user_attributes[target_field] = record[source_fields.upper()]
            mbatch.user_attributes = user_attributes
            sf_logger.debug(f"User attributes built: {mbatch.user_attributes}")

        app_event = mparticle.AppEvent(**data_plan_event.get("event_config"))
        mbatch.events = [app_event]
    else:
        json_payload = json.loads(record.get("DATA"))
        mbatch = mparticle.Batch(**json_payload)
        mbatch.source_request_id = str(uuid4())

    return mbatch
