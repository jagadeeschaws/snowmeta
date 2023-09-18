"""mparticle functions"""

import os
import json
import time
import random
import queue
import traceback
import asyncio
from uuid import uuid4
from datetime import datetime

import aiohttp
import mparticle
import pandas as pd
from snowflake.snowpark import Session, dataframe

from src.data_processor.core.mp_defaults import MP_DEFAULT_VALUES
from src.utils.logr import sf_logger

# Set the concurrency limit
# concurrency_limit = 1000
# semaphore = asyncio.Semaphore(concurrency_limit)
# Create a custom TCPConnector with the desired connection pool size
# connector = aiohttp.TCPConnector(limit=150)
# timeout = aiohttp.ClientTimeout(total=10)


async def send_df_to_mp(mp_conf, source_df_xform, data_plan_event, func_args, payload_conf):
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
    BATCH_SIZE = 10000
    connector = aiohttp.TCPConnector(limit=50)
    timeout = aiohttp.ClientTimeout(total=60)
    try:
        # Create a list of coroutines for sending data asynchronously
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # async with RetryClient(connector=connector) as session:
            coroutines = []
            all_results = []
            for batch_source_df in source_df_xform.to_pandas_batches():

                sf_logger.info(f"batch size: {len(batch_source_df)}")
                batch_source_df = batch_source_df.replace({pd.NaT: None})

                for rec in batch_source_df.to_records(index=False):
                    record_count += 1
                    payload = await build_pay_load(
                        mp_conf.get("environment"),
                        data_plan_event,
                        func_args["column_mapping"],
                        rec,
                        mp_conf["restart_from_mp_sink"],
                    )
                    if record_count == 1:  # first record to handle auth exception
                        try:
                            await send_data(payload, payload_conf, session)
                        except Exception as e:
                            raise Exception(e) from e
                    else:
                        # # Acquire the semaphore before starting the coroutine
                        # async with semaphore:
                        coroutines.append(send_data(payload, payload_conf, session))
                if len(coroutines) >= BATCH_SIZE:
                    print('i am in')
                    # Execute the coroutines concurrently and handle exceptions
                    results = await asyncio.gather(*coroutines, return_exceptions=True)
                    all_results.extend(results)
                    coroutines = []
                else:
                    pass

            if len(coroutines) >= 0:
                print('i am in final')
                # Execute the coroutines concurrently and handle exceptions
                results = await asyncio.gather(*coroutines, return_exceptions=True)
                all_results.extend(results)

            for result in all_results:
                if isinstance(result, Exception):
                    # Handle exception or perform logging
                    sf_logger.error("An error occurred during data sending:", exc_info=result)

            await flush_failed_records(
                payload_conf["ss"],
                payload_conf["failed_table_name"],
                payload_conf["failed_records"],
                payload_conf["count_queue"],
                payload_conf["lock"],
            )

            failed_rec_count = await get_total_failed_rec_count(
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


async def flush_failed_records(
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
async def get_total_failed_rec_count(count_queue: queue.Queue):
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


async def wait_before_retry(wait_tm):
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


async def send_data(data, payload_conf, session):
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
    async def add_failed_record(error_status, error_reason=None):
        await failed_records.put(
            (
                datetime.now().strftime("%Y-%m-%d"),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                run_id,
                error_status,
                error_reason,
                # data.user_identities.email,
                data['user_identities']['email'],
                # data.to_dict(),
                data,
            )
        )
        if await failed_records.qsize() == batch_size:
            await flush_failed_records(
                payload_conf.get("ss"),
                payload_conf.get("failed_table_name"),
                failed_records,
                payload_conf.get("count_queue"),
                payload_conf.get("lock"),
            )
        sf_logger.warning(
            "Maximum retries reached. Record added to failed records queue"
        )

    # api_inst: mparticle.Configuration = payload_conf.get("api_inst")
    max_retries = payload_conf.get("max_retries")
    base_wait_time = payload_conf.get("base_wait_time")
    max_wait_time = payload_conf.get("max_wait_time")
    run_id = payload_conf.get("run_id")
    failed_records = payload_conf.get("failed_records")
    batch_size = payload_conf.get("batch_size")

    num_retries = 0
    wait_time = base_wait_time
    # print(data.to_dict())
    # print(type(data.to_dict()))
    # retry_options = ExponentialRetry(attempts=2, statuses={400, 429} | set(range(500, 600)))
    while num_retries <= max_retries:
        try:
            async with session.post(
                    url='https://s2s.mparticle.com/v2/events',
                    # json=data.to_dict(),
                    json=data,
                    auth=aiohttp.BasicAuth("us1-82678b06c9c60047b7b0464368300442",
                                           "lQEGEay_DvlvW5HrPHJANFTtOAFFxzT5zvZNjZ6iMS3q0nCswxejIclxQHUotZUQ"),
                    #timeout=10,
                    # retry_options=retry_options
                    # auth=aiohttp.BasicAuth("us1-8ac307b7dda17c45857afeef4da0bd0d",
                    #                        "vFiEWtolmC05Jvjl7ochrtyAzsEZA214xT2KGttMjcFF8aVLlSmrXwnMiMYpZ9kp")
            ) as response:
                if response.status in (200, 202):
                    num_retries = max_retries + 1  # Successful
                elif response.status == 429:

                        sf_logger.warning(f"mParticle API rate limit exceeded : {response.reason}")

                        if num_retries == max_retries + 1:
                            await add_failed_record(response.status, response.reason)
                        else:
                            sf_logger.warning(f"Retry attempt {num_retries} of {max_retries}")

                            retry_after = int(response.headers.get("Retry-After", "0"))

                            if retry_after > 0:
                                await wait_before_retry(retry_after)
                            else:
                                await wait_before_retry(min(wait_time * 2, max_wait_time))
                elif response.status == 400 or response.status // 100 == 5:
                    num_retries += 1
                    # Bad Request or Server error
                    sf_logger.warning(f"Received a {response.reason} error.")

                    if num_retries == max_retries + 1:
                        await add_failed_record(response.status, response.reason)
                    else:
                        sf_logger.warning(f"Retry attempt {num_retries} of {max_retries}")

                        await wait_before_retry(min(wait_time * 2, max_wait_time))
                else:
                    raise Exception(response.status)

        except aiohttp.ServerDisconnectedError as e:
            num_retries += 1
            sf_logger.warning(f"Server disconnected: {e}. Retrying ({num_retries}/{max_retries})")

            if num_retries <= max_retries:
                await wait_before_retry(min(wait_time * 2, max_wait_time))

        except aiohttp.ClientOSError as e:
            num_retries += 1
            sf_logger.warning(f"Server disconnected: {e}. Retrying ({num_retries}/{max_retries})")

            if num_retries <= max_retries:
                await wait_before_retry(min(wait_time * 2, max_wait_time))

        except asyncio.exceptions.TimeoutError as e:
            num_retries += 1
            sf_logger.warning(f"Server disconnected: {e}. Retrying ({num_retries}/{max_retries})")

            if num_retries <= max_retries:
                await wait_before_retry(min(wait_time * 2, max_wait_time))

        except Exception as e:
            # Unhandled exception
            raise Exception(f"Received a {e} error") from e


# Build the payload for mParticle API
async def build_pay_load(
    environment, data_plan_event, column_mapping, record, restart_mp_sink_flag
):
    """Build payload for mparticle"""
    if not restart_mp_sink_flag:
        # Data be associated with a user identity
        if "user_identities" in column_mapping:
            # Build the user identities from the record and column mapping
            identities = {
                str(target_field).lower(): record[str(src_field).upper()]
                for src_field, target_field in column_mapping["user_identities"].items()
            }
        else:
            identities = {}
            # mbatch.user_identities = mparticle.UserIdentities(**identities)
            # sf_logger.debug(f"User identities built: {mbatch.user_identities}")
        #
        # # User attributes
        # # The mParticle audience platform can be powered by only sending a combination of user attributes,
        # # used to describe segments of users, and device identities/user identities used to then target those users.
        #
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
        else:
            user_attributes = {}
            # mbatch.user_attributes = user_attributes
            # sf_logger.debug(f"User attributes built: {mbatch.user_attributes}")
        #
        # app_event = mparticle.AppEvent(**data_plan_event.get("event_config"))
        # mbatch.events = [app_event]
        mbatch = {'events': [{'data': {'timestamp_unixtime_ms': None,
                                       'event_id': None,
                                       'source_message_id': None,
                                       'session_id': None,
                                       'session_uuid': None,
                                       'custom_attributes': None,
                                       'location': None,
                                       'device_current_state': None,
                                       'custom_event_type': 'other',
                                       'event_name': 'test_load_event',
                                       'media_info': None,
                                       'custom_flags': None},
                              'event_type': 'custom_event'}],
                  'source_request_id': str(uuid4()),
                  'environment': environment,
                  'ip': None, 'schema_version': None, 'device_info': None, 'application_info': None,
                  'user_attributes': user_attributes,
                  'deleted_user_attributes': None,
                  'user_identities': identities,
                  'consent_state': None, 'api_key': None, 'mpid': None, 'mp_deviceid': None,
                  'context': {'data_plan': data_plan_event.get("data_plan")}}
    else:
        mbatch = json.loads(record.get("DATA"))
        mbatch['source_request_id'] = str(uuid4())

    return mbatch
