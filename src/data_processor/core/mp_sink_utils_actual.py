"""mparticle functions"""

import json
import time
import random
import queue
import traceback
import asyncio
from uuid import uuid4
from datetime import datetime

import aiohttp
import pandas as pd
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, VariantType
from snowflake.snowpark import Session

from src.data_processor.core.mp_defaults import MP_DEFAULT_VALUES
from src.utils.logr import sf_logger


async def refresh_sf_session(mp_start_time, payload_conf):
    """
    Check the session duration and refresh the session before timeout duration.

    Args:
        mp_start_time: Start time of mparticle sink functionality.
        payload_conf (dict): A dictionary containing information required for payload

    Returns: None
    """
    mp_duration_mins = (datetime.now() - mp_start_time).total_seconds() / 60
    sf_logger.debug(f"session duration from last refresh: {mp_duration_mins}")

    if (
            mp_duration_mins
            >= payload_conf["idle_timeout_mins"]
            - payload_conf["refresh_before_timeout_mins"]
    ):
        payload_conf["ss"].sql("select current_timestamp").collect()
        mp_start_time = datetime.now()
        sf_logger.info("Session has been refreshed")
    return mp_start_time


async def send_df_to_mp(
        mp_conf, source_df_xform, data_plan_event, func_args, payload_conf
):
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
    BATCH_SIZE = int(mp_conf.get("full_load_params", {}).get(
        "async_coroutine_batch_size", MP_DEFAULT_VALUES["ASYNC_COROUTINE_BATCH_SIZE"]
    ))
    print(BATCH_SIZE)
    connector = aiohttp.TCPConnector(
        limit=int(mp_conf.get(
            "connection_pool_size", MP_DEFAULT_VALUES["CONNECTION_POOL_SIZE"]
        ))
    )
    print(connector)
    timeout = aiohttp.ClientTimeout(
        total=int(mp_conf["full_load_params"].get(
            "async_client_timeout", MP_DEFAULT_VALUES["ASYNC_CLIENT_TIMEOUT"])
        )
    )
    print(timeout)
    payload_batch_size = int(mp_conf.get("full_load_params", {}).get(
        "async_payload_batch_size", MP_DEFAULT_VALUES["ASYNC_PAYLOAD_BATCH_SIZE"]
    ))
    print(payload_batch_size)
    sleep_time = int(mp_conf.get("full_load_params", {}).get("async_sleep_time", 3))
    print(sleep_time)
    mp_start_time = datetime.now()
    sf_logger.debug(
        f"Initial session start time: {mp_start_time},"
        f"session idle time out mins: {payload_conf['idle_timeout_mins']}, "
        f"session refresh before timeout mins: {payload_conf['refresh_before_timeout_mins']}"
    )
    try:
        # Create a list of coroutines for sending data asynchronously
        async with aiohttp.ClientSession(
                connector=connector, timeout=timeout
        ) as session:
            # async with RetryClient(connector=connector) as session:
            coroutines = []
            all_results = []
            payload_list = []

            for batch_source_df in source_df_xform.to_pandas_batches():
                sf_logger.info(f"batch size: {len(batch_source_df)}")
                batch_source_df = batch_source_df.replace({pd.NaT: None})

                # Refresh the session before specified timeout duration
                mp_start_time = await refresh_sf_session(mp_start_time, payload_conf)
                sf_logger.debug(f"Refreshed session start time: {mp_start_time}")

                for rec in batch_source_df.itertuples(index=False):
                    record_count += 1
                    print("Processing..")
                    payload_list.append(
                        await build_pay_load(
                            mp_conf.get("environment"),
                            data_plan_event,
                            func_args["column_mapping"],
                            rec,
                            mp_conf["restart_from_mp_sink"],
                        )
                    )
                    if len(payload_list) == payload_batch_size:
                        print("Inside payload_list process")
                        coroutines.append(
                            asyncio.create_task(
                                send_data(payload_list, payload_conf, session, mp_conf)
                            )
                        )
                        payload_list = []
                    else:
                        pass

                if len(coroutines) >= BATCH_SIZE:
                    print("Inside coroutine exec")
                    # Execute the coroutines concurrently and handle exceptions
                    results = await asyncio.gather(*coroutines, return_exceptions=True)
                    await asyncio.sleep(sleep_time)
                    all_results.extend(results)
                    coroutines = []
                else:
                    pass

            if len(payload_list) > 0:
                print("Inside payload")
                coroutines.append(
                    send_data(payload_list, payload_conf, session, mp_conf)
                )
                # Execute the coroutines concurrently and handle exceptions
                results = await asyncio.gather(*coroutines, return_exceptions=True)
                await asyncio.sleep(sleep_time)
                all_results.extend(results)

            for result in all_results:
                if isinstance(result, Exception):
                    # Handle exception or perform logging
                    sf_logger.error(
                        "An error occurred during data sending:", exc_info=result
                    )

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
            f"Data processing failed while sending to mparticle with following {e}"
        ) from e

    return {
        "threshold_percentage": func_args["mp_properties"].get(
            "THRESHOLD_PERCENTAGE",
            func_args["mp_properties"].get("threshold_percentage"),
        ),
        "total_records_processed": record_count,
        "failed_record_count": failed_rec_count,
    }


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
            fail_table_schema = StructType(
                [
                    StructField("request_date", StringType()),
                    StructField("request_ts", StringType()),
                    StructField("run_id", IntegerType()),
                    StructField("status_code", IntegerType()),
                    StructField("failed_reason", StringType()),
                    StructField("source_request_id", StringType()),
                    StructField("data", VariantType()),
                ]
            )
            failed_records_df = ss.create_dataframe(
                failed_records_batch,
                fail_table_schema,
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


async def send_data(data, payload_conf, session, mp_conf):
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

    def add_failed_record(error_status, error_reason):
        #print(data)
        print(error_status)
        print(error_reason)
        print(run_id)
        failed_records.put(
            (
                datetime.now().strftime("%Y-%m-%d"),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                run_id,
                error_status,
                error_reason,
                # data["user_identities"]["email"],
                # ",".join([d['user_identities']['email'] for d in data]),
                'testemail',
                data,
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

    # api_inst: mparticle.Configuration = payload_conf.get("api_inst")
    max_retries = payload_conf.get("max_retries")
    base_wait_time = payload_conf.get("base_wait_time")
    max_wait_time = payload_conf.get("max_wait_time")
    run_id = payload_conf.get("run_id")
    failed_records = payload_conf.get("failed_records")
    batch_size = payload_conf.get("batch_size")
    endpoint_url = mp_conf["full_load_params"].get(
        "endpoint_url", MP_DEFAULT_VALUES["ENDPOINT_URL"]
    )

    num_retries = 0
    wait_time = base_wait_time

    while num_retries <= max_retries:
        try:
            async with session.post(
                    url=endpoint_url,
                    json=data,
                    auth=aiohttp.BasicAuth(mp_conf["MP_API_KEY"], mp_conf["MP_API_SECRET"]),
                    timeout=mp_conf["full_load_params"]["async_client_timeout"],
            ) as response:
                if response.status in (200, 202):
                    num_retries = max_retries + 1  # Successful
                elif response.status == 429:
                    sf_logger.warning(
                        f"mParticle API rate limit exceeded : {response.reason}"
                    )

                    if num_retries == max_retries + 1:
                        add_failed_record(response.status, response.reason)
                    else:
                        sf_logger.warning(
                            f"Retry attempt {num_retries} of {max_retries}"
                        )

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
                        add_failed_record(response.status, response.reason)
                    else:
                        sf_logger.warning(
                            f"Retry attempt {num_retries} of {max_retries}"
                        )

                        await wait_before_retry(min(wait_time * 2, max_wait_time))
                else:
                    raise Exception(response.status)

        except aiohttp.ServerDisconnectedError as e:
            num_retries += 1
            sf_logger.warning(
                f"Server disconnected: {e}. Retrying ({num_retries}/{max_retries})"
            )

            if num_retries == max_retries + 1:
                add_failed_record(999, "ServerDisconnectedError")
            else:
                sf_logger.warning(f"Retry attempt {num_retries} of {max_retries}")

                await wait_before_retry(min(wait_time * 2, max_wait_time))

        except aiohttp.ClientOSError as e:
            num_retries += 1
            sf_logger.warning(
                f"ClientOSError: {e}. Retrying ({num_retries}/{max_retries})"
            )

            if num_retries == max_retries + 1:
                add_failed_record(999, "ClientOSError")
            else:
                sf_logger.warning(f"Retry attempt {num_retries} of {max_retries}")

                await wait_before_retry(min(wait_time * 2, max_wait_time))

        except asyncio.exceptions.TimeoutError as e:
            num_retries += 1
            sf_logger.warning(
                f"TimeoutError: {e}. Retrying ({num_retries}/{max_retries})"
            )

            if num_retries == max_retries + 1:
                add_failed_record(999, "TimeoutError")
            else:
                sf_logger.warning(f"Retry attempt {num_retries} of {max_retries}")

                await wait_before_retry(min(wait_time * 2, max_wait_time))

        except Exception as e:
            # Unhandled exception
            sf_logger.warning(f"Received unknown error at mparticle: {e}")
            #print(data)
            add_failed_record(999, str(e))

            # raise Exception(f"Received a {e} error") from e


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
                str(target_field).lower(): getattr(record, str(src_field).upper())
                for src_field, target_field in column_mapping["user_identities"].items()
            }
        else:
            identities = {}
        sf_logger.debug(f"User identities built: {identities}")

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
                    ) and getattr(record, source_fields.upper()) is not None:
                        user_attributes[target_field] = json.loads(
                            getattr(record, source_fields.upper())
                        )
                    else:
                        user_attributes[target_field] = getattr(record, source_fields.upper())
                else:
                    user_attributes[target_field] = getattr(record, source_fields.upper())
        else:
            user_attributes = {}

        sf_logger.debug(f"User attributes built: {user_attributes}")

        app_event_attributes = {**data_plan_event.get("event_config")}

        if "app_event_attributes" in column_mapping:
            for source_fields, target_field in column_mapping[
                "app_event_attributes"
            ].items():
                if column_mapping.get("datatype_mapping"):
                    if (
                            column_mapping["datatype_mapping"].get(source_fields.upper())
                            == "dict"
                    ) and getattr(record, source_fields.upper()) is not None:
                        app_event_attributes[target_field] = json.loads(
                            getattr(record, source_fields.upper())
                        )
                    else:
                        app_event_attributes[target_field] = getattr(record, source_fields.upper())
                else:
                    app_event_attributes[target_field] = getattr(record, source_fields.upper())

        if 'custom_event_type' in app_event_attributes:
            pass
        else:
            app_event_attributes.update({'custom_event_type': defaults})

        if 'event_type' in app_event_attributes:
            event_type = app_event_attributes['event_type']
            del app_event_attributes['event_type']
        else:
            event_type = defaults


        mbatch = {
            "events": [{"data": app_event_attributes, "event_type": event_type}],
            "source_request_id": str(uuid4()),
            "environment": environment,
            "user_attributes": user_attributes,
            "user_identities": identities,
            "context": {"data_plan": data_plan_event.get("data_plan")},
        }
    else:
        mbatch = json.loads(record.get("DATA"))
        mbatch["source_request_id"] = str(uuid4())

    return mbatch
