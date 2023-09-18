"""Function lib"""
# import os

import queue
import threading
import jinja2
import snowflake.snowpark.exceptions
from snowflake.snowpark import Session,  dataframe
import mparticle
from src.data_processor.core.mp_defaults import MP_DEFAULT_VALUES
from src.utils.logr import sf_logger, fw_logger
from src.data_processor.core.mp_sink_utils_batch_pd import (
# from src.data_processor.core.mp_sink_utils import (
    get_source_df,
    create_mp_configuration,
    get_data_plan_event,
    send_df_to_mp,
)


def resolve_runtime_params(query, df_dict):
    """
    This function resolves any runtime parameters in a SQL query string by rendering it with a Jinja template.

    Parameters:
        query (str): A SQL query string with placeholders for runtime parameters.
        df_dict (dict): A dictionary of parameters.

    Returns:
        str: The SQL query string with runtime parameters resolved and rendered.
    """
    if df_dict.get("runtime_params"):
        fw_logger.info("Resolving runtime parameters")
        template = jinja2.Template(query)
        query = template.render(df_dict.get("runtime_params", {}))

    return query


def check_datatype(source_df_xform):
    """
    Check the datatype of source field and
    construct a dictionary if its Boolean/Array/Map type
    """
    # Dictionary source column and its data type
    field_datatype_dict = {}
    for field in source_df_xform.schema.fields:
        if "ArrayType" in str(field.datatype):
            field_datatype_dict[field.name] = "list"
        elif "MapType" in str(field.datatype):
            field_datatype_dict[field.name] = "dict"

    return field_datatype_dict


# Define the DDL function that executes a query or a list of queries
def ddl(ss: Session, func_args: dict, df_dict: dict = None) -> dict:
    """
    This function executes a single query or a list of queries using Snowflake Snowpark.

    Parameters:
    ss (Session): The Snowflake Snowpark session object.
    func_args (dict): A dictionary of arguments for the DDL function.
    df_dict (dict, optional): A dictionary of dataframes.
    This argument is not used in this function.

    Returns:
    None
    """
    fw_logger.info("Executing 'ddl' with %s", func_args)
    try:
        # Check if the query is present in the function arguments
        if "query" in func_args:
            # Execute the query and collect the results
            if func_args.get("sub_func") == "create_view":
                create_view(ss, func_args, df_dict)
            else:
                # raise ValueError("test")
                ss.sql(func_args["query"]).collect()
                sf_logger.info(f"Executed query: {func_args['query']}")

        elif "query_list" in func_args:
            # Loop through the query_list and execute each query
            for query in func_args["query_list"]:
                ss.sql(query).collect()
                sf_logger.info(f"Executed query: {query}")
        # If neither query nor query_list is present, raise an error
        else:
            log_msg = "Invalid arguments for the DDL function, only `query` or `query_list` supported"
            fw_logger.error(log_msg)
    except snowflake.snowpark.exceptions.SnowparkSQLException as exc_ddl:
        log_msg = "Failure in `ddl`"
        fw_logger.error(log_msg)
        raise snowflake.snowpark.exceptions.SnowparkSQLException(log_msg) from exc_ddl
    except Exception as e_ddl:
        log_msg = f"Failure in `ddl` function with {e_ddl}"
        fw_logger.error(log_msg)
        raise snowflake.snowpark.exceptions.SnowparkSQLException(log_msg) from e_ddl

    return df_dict


def create_view(ss: Session, func_args: dict, df_dict: dict = None) -> dict:
    """
    This function creates a temporary view using a SQL query defined in the `func_args` dictionary.

    Parameters:
        ss (Session): The Snowflake Snowpark session object.
        func_args (dict): A dictionary containing the following keys:
            - `query`: a SQL query string with placeholders for runtime parameters.
        df_dict (dict, optional): A dictionary of dataframes. This argument is not used in this function.

    Returns:
        dict: The `df_dict` input argument. This function does not modify the `df_dict` argument.
    """
    fw_logger.info("Executing 'create_view' with %s", func_args)

    try:
        query = resolve_runtime_params(func_args["query"], df_dict)
        # Execute the query and collect the results
        ss.sql(query).collect()
        sf_logger.info(f"Executed query: {query}")
    except Exception as exc_view:
        log_msg = f"Failure in `create_view` function with {exc_view}"
        fw_logger.error(log_msg)
        raise snowflake.snowpark.exceptions.SnowparkSQLException(log_msg) from exc_view

    return df_dict


# Define the function that consumes the stream data
def consume_stream(ss: Session, func_args: dict, df_dict: dict) -> dict:
    """
    This function consumes data from a stream in Snowflake.

    Parameters:
    ss (Session): The Snowflake Snowpark session object.
    func_args (dict): A dictionary of arguments for the consume_stream function.
    df_dict (dict): A dictionary of dataframes.

    Returns:
    dict: The updated df_dict with the additional key "stream_has_data".
    """
    fw_logger.info(f"Executing `consume_stream` with {func_args}")
    try:
        if df_dict.get("restart_flag"):
            sf_logger.info("Processing the records from previous failed run")
        else:
            # Check if the stream has data
            data_exist_flag = ss.sql(
                f"select SYSTEM$STREAM_HAS_DATA('{func_args['stream_name']}') AS STREAM_FLAG"
            ).first()
            transient_table = func_args["transient_table"].split(".")
            if (
                ss.sql(
                    f"show tables like '{transient_table[-1]}'"
                    f" in {transient_table[0]}.{transient_table[1]}"
                ).count()
                > 0
            ):
                raise RuntimeError(
                    "Cannot proceed current execution while previous failed run data yet to be processed."
                )
            if data_exist_flag["STREAM_FLAG"]:
                # Execute the stream query and collect the results
                ss.sql(func_args["stream_query"]).collect()
                sf_logger.info(
                    f"Consumed stream data using the query: {func_args['stream_query']}"
                )
                df_dict.update({"stream_has_data": True})
            else:
                # If the stream has no data, print a message indicating that further steps
                # should be skipped
                fw_logger.warning("Source is empty. Skipping further steps")
                df_dict.update({"stream_has_data": False})
    except Exception as e:
        log_msg = f"Failure in `consume_stream` function with {e}"
        fw_logger.error(log_msg)
        raise Exception(log_msg) from e

    return df_dict


# Define the SQL function that executes a query and returns the output dataframe
def sql(ss: Session, func_args: dict, df_dict: dict) -> dict:
    """
    Executes an SQL query and returns the output dataframe.

    Parameters:
    - ss (Session): The Snowflake Snowpark session instance.
    - func_args (dict): A dictionary containing the following keys:
        - 'query': The SQL query to be executed.
        - 'output_df': The name of the output dataframe.
    - df_dict (dict): A dictionary of existing dataframes.

    Returns:
    - dict: A dictionary of existing dataframes including the output of the executed SQL query.
    """
    fw_logger.info(f"Executing `sql` with {func_args}")
    try:
        query = resolve_runtime_params(func_args["query"], df_dict)
        stage_output_df = ss.sql(query)
        df_dict.update({func_args["output_df"]: stage_output_df})
        sf_logger.info(
            f"Executed query: {func_args['query']}, Output dataframe: {func_args['output_df']}"
        )
    except Exception as e:
        log_msg = f"Failure in `sql` function with {e}"
        fw_logger.error(log_msg)
        raise Exception(log_msg) from e
    return df_dict


def s3_sink(func_args: dict, df_dict: dict) -> dict:
    fw_logger.info(f"Executing `mparticle sink` with {func_args}")
    sf_logger.info("Started executing mparticle function")

    # Get the source dataframe
    source_df: dataframe = df_dict[func_args["input_df"]]
    # Apply transformations
    source_df_xform: dataframe = source_df.selectExpr(func_args["select_exp"])

    print(source_df_xform.count())

    source_df_xform.write.copy_into_location(
        location=func_args["s3_properties"]["target_location"],
        partition_by=func_args["s3_properties"].get("partition_by"),
        file_format_name=func_args["s3_properties"].get("file_format_name"),
        file_format_type=func_args["s3_properties"].get("file_format_type"),
        format_type_options=func_args["s3_properties"].get("format_type_options"),
        header=func_args["s3_properties"].get("header", False),
        **func_args["s3_properties"].get("copy_options", {})
    )

    return df_dict


def sf_sink(_, func_args: dict, df_dict: dict) -> dict:
    fw_logger.info(f"Executing `snowflake sink` with {func_args}")
    sf_logger.info("Started executing snowflake sink function")

    # Get the source dataframe
    source_df: dataframe = df_dict[func_args["input_df"]]
    # Apply transformations
    source_df_xform: dataframe = source_df.selectExpr(func_args["select_exp"])

    print(source_df_xform.count())

    if func_args["sf_properties"].get("mode"):
        write_mode = source_df_xform.write.mode(func_args["sf_properties"]["mode"])
    else:
        write_mode = source_df_xform.write

    write_mode.saveAsTable(func_args["sf_properties"]["table"], table_type=func_args["sf_properties"].get("table_type",""))

    return df_dict


def mp_sink(ss: Session, func_args: dict, df_dict: dict) -> dict:
    """
    Sends data from a Snowflake DataFrame to mParticle.

    This function takes a `Session` object, a dictionary of arguments `func_args`, and a dictionary of DataFrames
    `df_dict` as input. It returns a dictionary of results.

    Args: _:
    Session: A `Session` object that represents a connection to a Snowflake account.
    func_args (dict): A dictionary of arguments that contains configuration information for the function.
    df_dict (dict): A dictionary of DataFrames, where each key is a string that represents the name of the DataFrame,
                    and the value is a DataFrame.

    Returns:
        dict: A dictionary of results.
    """
    fw_logger.info(f"Executing `mparticle sink` with {func_args}")
    sf_logger.info("Started executing mparticle function")

    mp_conf = df_dict.get("mparticle", {})

    fail_rec_table_name = func_args["mp_properties"].get(
        "FAIL_REC_TABLE_NAME", func_args["mp_properties"].get("fail_rec_table_name")
    )

    # Get Source Data
    source_df_xform, restart_from_mp_sink = get_source_df(
        ss, df_dict, func_args, fail_rec_table_name
    )

    mp_conf.update({"restart_from_mp_sink": restart_from_mp_sink})

    # Get Data plan and event
    data_plan_event = get_data_plan_event(func_args["mp_properties"])

    # Exponential backoff pattern configuration
    exp_backoff_conf = mp_conf.get("exp_backoff", {})

    field_datatype_dict = check_datatype(source_df_xform)

    payload_conf = {
        "ss": ss,
        "api_inst": mparticle.EventsApi(
            create_mp_configuration(mp_conf)
        ),  # Create an EventsApi object
        "max_retries": exp_backoff_conf.get(
            "max_retries", MP_DEFAULT_VALUES["MAX_RETRIES"]
        ),
        "base_wait_time": exp_backoff_conf.get(
            "base_wait_timee", MP_DEFAULT_VALUES["BASE_WAIT_TIME"]
        ),
        "max_wait_time": exp_backoff_conf.get(
            "max_wait_time", MP_DEFAULT_VALUES["MAX_WAIT_TIME"]
        ),
        "run_id": df_dict["run_id"],
        "failed_table_name": fail_rec_table_name,
        "failed_records": queue.Queue(),  # Create a queue for failed records
        "count_queue": queue.Queue(),  # Create a queue to store the counts
        "lock": threading.Lock(),  # Create a lock object
        "batch_size": mp_conf.get(
            "failed_record_queue_size", MP_DEFAULT_VALUES["QUEUE_SIZE"]
        ),
        "field_datatype_dict": field_datatype_dict,
    }

    func_args["column_mapping"]["datatype_mapping"] = payload_conf[
        "field_datatype_dict"
    ]
    mp_func_output = send_df_to_mp(
        mp_conf, source_df_xform, data_plan_event, func_args, payload_conf
    )
    # Update the df_dict with total record count and failed record count
    df_dict.update(mp_func_output)

    return df_dict
