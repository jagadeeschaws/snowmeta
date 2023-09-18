"""
Script: metadata_process.py
Purpose: This is the driver module for generation of metadata.
         It contains functions for creation of stream,
         warehouse and generation of data flow pipeline
"""

import json
import sys
import traceback
import data_flow_utils
from meta_logr import sf_logger as logger, set_logger, get_logs_list, clear_log


def retrieve_object_attr(ss, process_name: str):
    """
    Retrieve the object information from object attributes table

    Args:
    ss (Snowpark Session): snowpark session object
    process_name (str): name of the process for which metadata generation is triggered

    Returns:
    None

    """
    # Check if there is a duplicate entry for the process in the object attributes table,
    # otherwise retrieve the data from the table
    object_attr_data = ss.table("object_attr").where(
        f"lower(process_name) = lower('{process_name}')"
    )
    if object_attr_data.count() > 1:
        raise ValueError(
            f"Duplicate entry found in Object Attributes for the process {process_name}. "
            f"Please check the configuration and try again."
        )
    if object_attr_data.count() == 0:
        raise ValueError(
            f"Data not found in object attributes table for the process {process_name}"
        )

    object_data = object_attr_data.first().as_dict()
    logger.info(f"Retrieved object attributes for the process {process_name}")
    return object_data


def retrieve_warehouse_attributes(ss, deploy_attr_stream_dict: dict, object_data: dict):
    """
    Check the create warehouse flag and retrieve warehouse attributes

    Args:
      ss (Snowpark Session): snowpark session object
      object_data (dict): dictionary of object attributes data
      deploy_attr_stream_dict (dict): dictionary of deploy attributes stream data

    Returns:
       dataframe: warehouse_attributes
    """
    warehouse_attributes = ""
    if deploy_attr_stream_dict["CREATE_WH_FLAG"]:
        warehouse_attributes_df = (
            ss.table("warehouse_attr")
            .where(
                f"lower(compute_wh_name) = lower('{object_data['COMPUTE_WH_NAME']}')"
            )
            .first()
        )
        if warehouse_attributes_df:
            warehouse_attributes = warehouse_attributes_df.as_dict()
        else:
            raise ValueError(
                f"If the CREATE_WH_FLAG is set to True, "
                f"the WAREHOUSE_ATTR table must contain the necessary data to create a warehouse."
                f" Please provide the required information in the WAREHOUSE_ATTR table for "
                f"{object_data['COMPUTE_WH_NAME']}."
            )
    return warehouse_attributes


def change_to_data_role(ss, object_data: dict):
    """
    Change the role from framework role to data role if provided in object attributes.

    Args:
    ss (Snowpark Session): snowpark session object
    object_data (dict): dictionary of object attributes data

    Returns:
    str: current role

    """
    # Get current role
    current_role = ss.get_current_role()
    current_warehouse = ss.get_current_warehouse()
    logger.info(f"current role before switch:{current_role}")
    logger.info(f"current warehouse before switch:{current_warehouse}")
    # Change the role to data role to create warehouse/stream if provided in object attributes
    if object_data.get("ROLE"):
        try:
            ss.sql(f"USE ROLE {object_data['ROLE']}").collect()
            logger.info(f"Switched the role to {object_data['ROLE']}")
        except Exception as e:
            logger.error(e)
            raise ValueError(
                f"Failure in setting up data role {object_data['ROLE']}"
            ) from e

    return current_role, current_warehouse


# Method to generate query for creating warehouse
def process_warehouse_object(
    ss, object_data: dict, deploy_attr_stream_dict: dict, warehouse_attributes
):
    """
    Create warehouse with the provided warehouse properties.

    Args:
    ss (Snowpark Session): snowpark session object
    object_data (dict): dictionary of object attributes data
    deploy_attr_stream_dict (dict): dictionary of deploy attributes stream data

    Returns:
    None

    """

    def create_or_update_warehouse(wh_attr: dict):
        actual_query = f"CREATE OR REPLACE WAREHOUSE {wh_attr['COMPUTE_WH_NAME']}"
        query = ""
        # Construct the create warehouse query using the provided properties
        if wh_attr.get("COMPUTE_WH_PROPERTIES"):
            wh_properties = json.loads(wh_attr["COMPUTE_WH_PROPERTIES"])
            for key, value in wh_properties.items():
                if key.lower() == "comment":
                    query = query + f" {key}='{str(value)}'"
                else:
                    query = query + f" {key}={str(value)}"
            warehouse_query = actual_query + " WITH " + query
        else:
            logger.info(
                f"Creating/Updating warehouse with default properties as warehouse "
                f"properties are not provided for {wh_attr['COMPUTE_WH_NAME']}"
            )
            warehouse_query = actual_query
        logger.debug(f"Warehouse DDL is {warehouse_query}")
        ss.sql(warehouse_query).collect()

    # Entry to this method
    # Check if the create warehouse flag is set to True
    if deploy_attr_stream_dict.get("CREATE_WH_FLAG"):
        create_or_update_warehouse(warehouse_attributes)
        logger.info(f"Created/Updated the warehouse {object_data['COMPUTE_WH_NAME']}")

    else:
        # Check if warehouse exists
        warehouse_sql = f"use warehouse {object_data['COMPUTE_WH_NAME']}"
        logger.debug(f"warehouse_sql is {warehouse_sql}")
        try:
            ss.sql(warehouse_sql).collect()
            logger.info(f"current warehouse after switch: {ss.get_current_warehouse()}")
        except Exception as e:
            logger.error(e)
            raise ValueError(
                f"The `CREATE_WH_FLAG` flag in the deploy attribute table is set to False, "
                f"and the warehouse {object_data['COMPUTE_WH_NAME']} does not exist or "
                f"role {object_data['ROLE']} does not have appropriate privileges to the warehouse. "
                f"This will impact data processing. Please create the warehouse "
                f"and re-initiate the metadata deployment process."
            ) from e


# Methods to generate query for creating stream
def get_stream_statement(stream_prop: dict) -> str:
    """
    Generate statement with the provided stream properties.

    Args:
        stream_prop (dict): dictionary of stream properties

    Returns:
        str: resolved query statement

    """
    statement = ""
    for k, v in stream_prop.items():
        if k.lower() == "comment":
            statement = statement + f" {k}='{str(v)}'"
        else:
            statement = statement + f" {k}={str(v)}"
    return statement


def process_stream_object(ss, object_data: dict, deploy_attr_stream_dict: dict):
    """
    Create stream with the provided stream properties.

    Args:
    ss (Snowpark Session): snowpark session object
    object_data (dict): dictionary of object attributes data
    deploy_attr_stream_dict (dict): dictionary of deploy attributes stream data

    Returns:
    None

    """

    def create_stream():
        # Construct DDL for stream
        stream_ddl_statement = (
            f"CREATE OR REPLACE STREAM " f"{object_data['STREAM_NAME']} ON TABLE "
        )

        # Check if object type is table/view
        if object_data["OBJECT_TYPE"].lower() == "table":
            stream_ddl_statement += str(
                f"{object_data['DATABASE_NAME']}."
                f"{object_data['SCHEMA_NAME']}."
                f"{object_data['OBJECT_NAME']}"
            )
        elif object_data["OBJECT_TYPE"].lower() == "view":
            if object_data.get("PRIMARY_TABLE"):
                stream_ddl_statement += str(
                    f"{object_data['DATABASE_NAME']}."
                    f"{object_data['SCHEMA_NAME']}."
                    f"{object_data['PRIMARY_TABLE']}"
                )
            else:
                raise ValueError(
                    "Incremental capture flag is true & object type is view. "
                    "Please provide the primary table to create stream"
                )
        else:
            raise ValueError("Invalid object type.")

        # Check if stream properties are not provided, create with default properties
        if not object_data.get("STREAM_PROPERTIES"):
            create_stream_query = stream_ddl_statement + " append_only = True"
            logger.info(
                "Stream properties not provided; "
                "creating the stream with default configuration "
                "(Append_Only=True)."
            )
        else:
            stream_prop = json.loads(object_data["STREAM_PROPERTIES"])
            # Check if time_travel is provided in stream properties and construct the time travel statement
            if "time_travel" in [key.lower() for key in stream_prop.keys()]:
                tt_statement = ""
                for key, value in stream_prop["time_travel"].items():
                    tt_statement = tt_statement + " " + key + "(" + value + ")"
                stream_prop.pop("time_travel")
                stream_prop_statement = tt_statement + get_stream_statement(stream_prop)
            else:
                stream_prop_statement = get_stream_statement(stream_prop)

            create_stream_query = stream_ddl_statement + stream_prop_statement
        logger.debug(f"Stream DDL is {create_stream_query}")
        ss.sql(create_stream_query).collect()

    # Entry to this method
    # Checks if the given stream exists, and if it does not, it creates the stream.
    # If the stream already exists, it checks whether the option to replace the existing stream
    # is set to True, and if so, it replaces the stream.
    if object_data["STREAM_NAME"]:
        stream_namespace = object_data["STREAM_NAME"].split(".")
        if len(stream_namespace) == 3:
            stream_exists_query = (
                f"show streams like "
                f"'{stream_namespace[-1]}' in {stream_namespace[0]}.{stream_namespace[1]}"
            )
            logger.debug(f"stream_exists_query is {stream_exists_query}")
            stream_exists = ss.sql(stream_exists_query).count()

            if stream_exists == 0:
                create_stream()
                logger.info(f"Created new stream {object_data['STREAM_NAME']}")
            else:
                if deploy_attr_stream_dict["REPLACE_EXISTING_STREAM"]:
                    create_stream()
                    logger.info(
                        f"Successfully updated stream {object_data['STREAM_NAME']}."
                    )
                else:
                    logger.info(
                        f"The stream {object_data['STREAM_NAME']} already exists and "
                        f"'replace_existing_stream' is set to False in deploy attribute table. "
                        f"No changes were made to the existing stream."
                    )
        else:
            raise ValueError("Namespace should be provided for stream name ")
    else:
        raise ValueError(
            f"Incremental capture flag is True,"
            f"but stream name is not provided for process: {object_data['PROCESS_NAME']} "
        )


def persist_pipeline(
    ss, process_name: str, pipeline_config: json, deploy_attr_stream_dict: dict
):
    """
    Update pipeline table with the pipeline config.

    Args:
    ss (Snowpark Session): snowpark session object
    process_name (str): name of the process for which metadata generation is triggered
    pipeline_config (json): json containing steps for data process
    deploy_attr_stream_dict (dict): dictionary of deploy attributes stream data

    Returns:
    None

    """
    # Persist data pipeline into pipeline table
    pipeline_exists = (
        ss.table("pipeline")
        .where(f"lower(process_name) = lower('{process_name}')")
        .first()
    )

    if not pipeline_exists:
        ss.sql(
            f"INSERT INTO PIPELINE "
            f"(PROCESS_NAME, PIPELINE_CONFIG, RUNTIME_PARAMS, COMMENT_TEXT, CREATE_USER, CREATE_DATE_TIME, "
            f"UPDATE_USER, UPDATE_DATE_TIME)"
            f"SELECT '{process_name}'"
            f",parse_json($${pipeline_config}$$)"
            f",NULL"
            f",'{deploy_attr_stream_dict['COMMIT_MESSAGE']}'"
            f",current_user()"
            f",current_timestamp()"
            f",current_user()"
            f",current_timestamp()"
        ).collect()
        logger.info(
            f"Pipeline configuration has been inserted for the process '{process_name}'."
        )
    else:
        ss.sql(
            f"UPDATE PIPELINE "
            f"SET PIPELINE_CONFIG = parse_json($${pipeline_config}$$)"
            f", UPDATE_DATE_TIME= current_timestamp() "
            f", UPDATE_USER = current_user() "
            f", COMMENT_TEXT = '{deploy_attr_stream_dict['COMMIT_MESSAGE']}'"
            f" WHERE lower(PROCESS_NAME)=lower('{process_name}')"
        ).collect()
        logger.info(
            f"Updated pipeline configuration for process {process_name} in pipeline table"
        )


def metadata_generation(ss, log_level: str):
    """
    Generate metadata using the information from metadata tables.

    Args:
    ss (Snowpark Session): snowpark session object
    log_level (str): Log level for logger (INFO, DEBUG, WARNING, ERROR)

    Returns:
    None
    """

    process_name = ""
    deploy_status = ""
    seq_no = ""
    current_role = ""
    current_warehouse = ""
    status_dict = {}
    set_logger(log_level)

    # Read the data from deploy attribute stream
    logger.info("Reading data from deploy attr table stream")
    deploy_attr_stream = ss.table("deploy_attr_stream").collect()
    if not deploy_attr_stream:
        logger.warning("Deploy attribute stream is empty")
    else:
        # Looping through the deploy attribute stream.
        for record in deploy_attr_stream:
            try:
                deploy_attr_stream_dict = record.as_dict()
                process_name = deploy_attr_stream_dict["PROCESS_NAME"]
                seq_no = deploy_attr_stream_dict["SEQUENCE_NUMBER"]
                logger.info(
                    f"Metadata generation has started for process {process_name}"
                )
                logger.info(
                    f"Retrieved record from deploy attribute stream. "
                    f"Deploying metadata configuration for `{process_name}` ..."
                )

                # Retrieve object attributes
                object_data = retrieve_object_attr(ss, process_name)

                # Retrieve warehouse attributes if create warehouse flag is true
                warehouse_attributes = retrieve_warehouse_attributes(
                    ss, deploy_attr_stream_dict, object_data
                )

                # Switch the role from framework role to data role to create stream/warehouse objects
                current_role, current_warehouse = change_to_data_role(ss, object_data)

                # Creating warehouse if create warehouse flag is enabled
                process_warehouse_object(
                    ss, object_data, deploy_attr_stream_dict, warehouse_attributes
                )

                # Creating stream if incremental_capture_flag is enabled
                if object_data["INCREMENTAL_CAPTURE_FLAG"]:
                    process_stream_object(ss, object_data, deploy_attr_stream_dict)
                else:
                    logger.info(
                        "INCREMENTAL_CAPTURE_FLAG is False, hence skipping the stream creation"
                    )

                # Change the role back from data role to framework role
                if current_role != ss.get_current_role():
                    ss.sql(f"USE ROLE {current_role}").collect()
                    ss.sql(f"USE WAREHOUSE {current_warehouse}").collect()
                    logger.info(
                        f"Switched the role back to {current_role} and warehouse to {current_warehouse}"
                    )

                # Constructing data pipeline config
                pipeline_config = data_flow_utils.construct_pipeline_json(
                    ss, process_name, object_data
                )
                # Persist data pipeline into pipeline table
                persist_pipeline(
                    ss, process_name, pipeline_config, deploy_attr_stream_dict
                )

                logger.info(
                    f"Metadata generation has been completed successfully for process {process_name}"
                )
                deploy_status = "SUCCEEDED"
                status_dict.update({process_name: {"status": deploy_status}})
            except Exception as e:
                if current_role:
                    ss.sql(f"USE ROLE {current_role}").collect()
                    ss.sql(f"USE WAREHOUSE {current_warehouse}").collect()
                logger.error(traceback.format_exc())
                logger.error(
                    f"Metadata generation failed for process {process_name}: {e}. "
                    f"Please check deploy_attr for more details."
                )
                deploy_status = "FAILED"
                status_dict.update(
                    {process_name: {"status": deploy_status, "error_message": e}}
                )

            finally:
                # Get the logs and update deploy attribute table
                final_log_list = get_logs_list().replace("'", "''")
                ss.sql(
                    f"UPDATE DEPLOY_ATTR "
                    f"SET STATUS = '{deploy_status}'"
                    f", LOG_MESSAGE = '{final_log_list}'"
                    f", COMPLETION_DATE_TIME = current_timestamp()"
                    f", UPDATE_USER = current_user()"
                    f" WHERE lower(PROCESS_NAME) =lower('{process_name}')"
                    f" AND SEQUENCE_NUMBER = {seq_no}"
                ).collect()

                clear_log()

        ss.sql(
            "create or replace temporary table deploy_attr_stream_temp as select * from deploy_attr_stream"
        ).collect()

    return status_dict


# Entry point of the script
def run_metadata_process(ss, log_level: str):
    """
    Trigger metadata generation.

    Args:
    ss (Snowpark Session): snowpark session object
    log_level (str): Log level for logger (INFO, DEBUG, WARNING, ERROR)

    Returns:
    None
    """
    status_dict = metadata_generation(ss, log_level)
    try:
        is_failed_status = False
        for _, status in status_dict.items():
            if status.get("status") == "FAILED":
                is_failed_status = True
        if is_failed_status:
            raise Exception(
                f"One or more processes have encountered metadata generation "
                f"failure with the following status: {status_dict}. "
                f"Please refer to the deploy_attr table for additional information."
            )

        return status_dict

    except Exception:
        sys.exit(1)
