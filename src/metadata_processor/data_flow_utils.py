"""
Script: data_flow_utils.py
Purpose: This module contains functions to construct pipeline json for a particular process
"""

import json
from meta_logr import sf_logger as logger
import dynamic_view_query_generation


def consume_stream(object_data: dict, step_id: int) -> dict:
    """
    Data flow step to consume stream.

    Args:
    object_data (dict): dictionary of object attributes data
    step_id (int):  represents each step with unique value

    Returns:
    dict: steps for consuming stream

    """
    # Construct query to create transient table
    if object_data.get("TRANSIENT_STG_TABLE"):
        if len(object_data["TRANSIENT_STG_TABLE"].split(".")) == 3:
            stream_query = (
                f"create transient table if not exists {object_data['TRANSIENT_STG_TABLE']} "
                f"as select * from {object_data['STREAM_NAME']}"
            )
        else:
            raise ValueError("Namespace should be provided for Transient stage table")
    else:
        raise ValueError(
            "Incremental capture flag is true, hence transient stage table name cannot be empty"
        )

    read_stream = {
        "step_id": step_id,
        "desc": "consume stream by loading data into transient stage table",
        "func": "consume_stream",
        "stream_name": object_data["STREAM_NAME"],
        "stream_query": stream_query,
        "transient_table": object_data["TRANSIENT_STG_TABLE"],
    }
    return read_stream


def create_dynamic_view(
    session,
    object_data: dict,
    step_id: int,
) -> dict:
    """
    Data flow step to create dynamic view.

    Args:
    session (Snowpark Session): snowpark session object
    object_data (dict): dictionary of object attributes data
    step_id (int):  represents each step with unique value

    Returns:
    dict: steps for creating dynamic view

    """
    view_name = object_data["OBJECT_NAME"]
    primary_table = object_data["PRIMARY_TABLE"]
    transient_stg_table = object_data.get("TRANSIENT_STG_TABLE")
    incremental_flag = object_data["INCREMENTAL_CAPTURE_FLAG"]
    # Generate dynamic view query
    view_query = dynamic_view_query_generation.create_dynamic_view_query(
        session, view_name, primary_table, transient_stg_table, incremental_flag
    )

    dynamic_view_query = {
        "step_id": step_id,
        "desc": "creating dynamic view",
        "func": "ddl",
        "sub_func": "create_view",
        "query": f"{view_query}",
    }
    return dynamic_view_query


def extract_data(object_data: dict, step_id: int) -> dict:
    """
    Data flow step to extract data from table to dataframe.

    Args:
    object_data (dict): dictionary of object attributes data
    step_id (int):  represents each step with unique value

    Returns:
    dict: steps for extracting data

    """
    # Construct the query to extract data based on object type
    object_name = object_data["OBJECT_NAME"]
    if object_data["OBJECT_TYPE"].lower() == "view":
        extract_query = f"select * from {object_name}"
        descr = "view"
    else:
        if object_data["INCREMENTAL_CAPTURE_FLAG"]:
            extract_query = f"select * from {object_data['TRANSIENT_STG_TABLE']}"
            descr = "transient table"

        else:
            extract_query = f"select * from {object_name}"
            descr = "table"

    data_extract = {
        "step_id": step_id,
        "desc": f"create dataframe extracting data from {descr}",
        "func": "sql",
        "query": extract_query,
        "output_df": f"{object_name}_df",
    }
    return data_extract


def mp_sink(
    column_map: dict,
    select_exp_list: list,
    target_prop: dict,
    step_id: int,
    object_name: str,
) -> dict:
    """
    Data flow step to construct payload for mparticle from the dataframe.

    Args:
    column_map (dict): dictionary of target columns grouped by  mparticle object categories
    select_exp_list (list): list containing source and target column mapping
    target_prop (dict): dictionary containing target properties
    step_id (int):  represents each step with unique value
    object_name (str): name of object provided in object attr table

    Returns:
    dict: steps for constructing mparticle payload

    """

    load_mp_sink = {
        "step_id": step_id,
        "desc": "convert dataframe to payload and send data to mparticle",
        "func": "mp_sink",
        "input_df": f"{object_name}_df",
        "select_exp": select_exp_list,
        "mp_properties": target_prop,
        "column_mapping": column_map,
    }
    return load_mp_sink


def sf_sink(
    select_exp_list: list,
    target_prop: dict,
    step_id: int,
    object_name: str,
) -> dict:

    load_sf_sink = {
        "step_id": step_id,
        "desc": "writing data into Snowflake table",
        "func": "sf_sink",
        "input_df": f"{object_name}_df",
        "select_exp": select_exp_list,
        "sf_properties": target_prop
    }
    return load_sf_sink


def drop_transient_table(object_data: dict, step_id: int) -> dict:
    """
    Data flow step to Drop the transient table.

    Args:
    object_data (dict): dictionary of object attributes data
    step_id (int):  represents each step with unique value

    Returns:
    dict: steps to drop transient table

    """

    drop_query = f"drop table if exists {object_data['TRANSIENT_STG_TABLE']}"

    drop_trans_table = {
        "step_id": step_id,
        "desc": "Drop the transient table",
        "func": "ddl",
        "query": drop_query,
    }
    return drop_trans_table


def mp_column_mapping(session, process_name: str, target_name: str):
    """
    Create Column mapping from source to target.

    Args:
    session (Snowpark Session): snowpark session object
    process_name (str): name of the process for which metadata generation is triggered
    target_name (str): Name of the target

    Returns:
    list: list containing source and target column mapping
    dict: dictionary of target columns grouped by  mparticle object categories

    """

    # Get data from column attributes table for matching target
    col_attr_df = (
        session.table("column_attr")
        .where(f"lower(process_name) = lower('{process_name}')")
        .where(f"lower(target_name) = lower('{target_name}')")
        .collect()
    )

    if col_attr_df:
        column_map = {}
        select_expr = ["cast(1 as integer) as dummy_num_column"]
        for record in col_attr_df:
            column_attr = record.as_dict()
            if column_attr.get("TARGET_COLUMN_NAME"):
                source_column_name = column_attr["COLUMN_NAME"]
                transform_rule = column_attr["TRANSFORM_RULE"]

                if column_attr.get("OBJECT_CATEGORY"):
                    object_category = column_attr["OBJECT_CATEGORY"].lower()
                    target_column_name = column_attr["TARGET_COLUMN_NAME"]
                    if target_column_name.startswith("$"):
                        column_name = target_column_name[1:] + "_" + object_category
                    else:
                        column_name = target_column_name + "_" + object_category

                    if object_category in column_map:
                        column_map[object_category][column_name] = target_column_name
                    else:
                        column_map.update(
                            {object_category: {column_name: target_column_name}}
                        )
                else:
                    raise ValueError(
                        "Object category cannot be null for target mparticle"
                    )

                # Construct select expression based on transform rule
                if transform_rule:
                    select_expr.append(f"{transform_rule} AS {column_name}")
                else:
                    select_expr.append(f"{source_column_name} AS {column_name}")
    else:
        raise ValueError(
            f"Column attributes not found for the process {process_name}, target {target_name}"
        )

    return column_map, select_expr


def get_select_exp_list(session, process_name: str, target_name: str):

    # Get data from column attributes table for matching target
    col_attr_df = (
        session.table("column_attr")
        .where(f"lower(process_name) = lower('{process_name}')")
        .where(f"lower(target_name) = lower('{target_name}')")
        .collect()
    )

    if col_attr_df:
        select_expr = []
        for record in col_attr_df:
            column_attr = record.as_dict()
            if column_attr.get("TARGET_COLUMN_NAME"):
                source_column_name = column_attr["COLUMN_NAME"]
                target_column_name = column_attr["TARGET_COLUMN_NAME"]
                transform_rule = column_attr["TRANSFORM_RULE"]

                # Construct select expression based on transform rule
                if transform_rule:
                    select_expr.append(f"{transform_rule} AS {target_column_name}")
                else:
                    select_expr.append(f"{source_column_name} AS {target_column_name}")
    else:
        raise ValueError(
            f"Column attributes not found for the process {process_name}, target {target_name}"
        )

    return select_expr

def get_mp_target_properties(
    target_attr, process_name: str, target_name: str, object_data: dict
) -> dict:
    """
    Get target properties from target_attr table and add name space to failed record table.

    Args:
    target_attr (dataframe): dataframe of target attributes data
    process_name (str): name of the process for which metadata generation is triggered
    target_name (str): Name of the target
    object_data (dict): dictionary of object attributes data

    Returns:
    dict: dictionary of target properties for a particular process and target

    """
    if target_attr:
        target_attr_dict = target_attr.as_dict()
        logger.info(
            f"Target attributes retrieved for process {process_name}, target {target_name}"
        )
        if target_attr_dict.get("TARGET_PROPERTIES"):
            target_prop = json.loads(target_attr_dict["TARGET_PROPERTIES"])
            # Check for mandatory keys in target properties
            mandt_target_prop_key_list = [
                "PLAN_ID",
                "FAIL_REC_TABLE_NAME",
                "EVENT_NAME",
            ]
            if not all(
                valid_key.lower()
                in [target_key.lower() for target_key in target_prop.keys()]
                for valid_key in mandt_target_prop_key_list
            ):
                raise KeyError(
                    "Mandatory keys `plan_id`,`event_name`,`fail_rec_table_name` are missing in Target properties."
                )
            # Add name space to fail record table name if not provided
            fail_rec_table_key = ""
            for key in target_prop.keys():
                if "fail_rec_table_name" == key.lower():
                    fail_rec_table_key = key
                    break
            if len(target_prop.get(fail_rec_table_key).split(".")) != 3:
                target_prop[fail_rec_table_key] = (
                    object_data["DATABASE_NAME"]
                    + "."
                    + object_data["SCHEMA_NAME"]
                    + "."
                    + target_prop[fail_rec_table_key].split(".")[-1]
                )
        else:
            raise ValueError(
                f"Target properties not provided for process {process_name}, target {target_name}"
            )
    else:
        raise ValueError(
            f"Target attributes not found for process {process_name}, target {target_name}"
        )
    return target_prop


def construct_data_flow(session, process_name: str, object_data: dict) -> list:
    """
    Construct dataflow list.

    Args:
    session (Snowpark Session): snowpark session object
    process_name (str): name of the process for which metadata generation is triggered
    object_data (dict): dictionary of object attributes data

    Returns:
    list: list containing steps for data flow

    """
    step_id = 0
    data_flow_list = []

    # Increment the step_id and include consume stream step to data flow
    # if incremental capture flag is set to True
    if object_data["INCREMENTAL_CAPTURE_FLAG"]:
        step_id = step_id + 100
        data_flow_list.append(consume_stream(object_data, step_id))
    else:
        pass

    # Increment the step_id and include creating dynamic view step to data flow, if object type is view
    if object_data["OBJECT_TYPE"].lower() == "view":
        step_id = step_id + 100
        data_flow_list.append(
            create_dynamic_view(
                session,
                object_data,
                step_id,
            )
        )
    else:
        pass

    # Increment the step_id and include extracting data to dataframe step to data flow
    step_id = step_id + 100
    data_flow_list.append(extract_data(object_data, step_id))

    # Get the list of targets from object attributes
    targets_list = json.loads(object_data["TARGET_NAME_LIST"])
    if len(targets_list) == 0:
        raise ValueError(
            "Target name list is empty in object attributes table."
            " Please provide atleast one target name."
        )

    for target_name, target in targets_list.items():
        # Get data from target attributes table
        target_attr = (
            session.table("target_attr")
            .where(f"lower(process_name) = lower('{process_name}')")
            .where(f"lower(target_name) = lower('{target_name}')")
            .first()
        )

        # Increment the step_id
        step_id = step_id + 100

        if target.lower() == "mparticle":

            target_prop = get_mp_target_properties(
                target_attr, process_name, target_name, object_data
            )

            # Construct source to target column mapping
            col_map, select_exp_list = mp_column_mapping(
                session, process_name, target_name
            )

            data_flow_list.append(
                mp_sink(
                    col_map,
                    select_exp_list,
                    target_prop,
                    step_id,
                    object_data["OBJECT_NAME"],
                )
            )
        elif target.lower() == "snowflake":
            data_flow_list.append(
                sf_sink(
                    get_select_exp_list(session, process_name, target_name),
                    target_prop,
                    step_id,
                    object_data["OBJECT_NAME"]
                )
            )
        else:
            raise ValueError(
                f"Invalid target {target}.Only mparticle is supported currently"
            )

    # Increment the step_id and include dropping transient table step to data flow
    # if incremental captur flag is True
    if object_data["INCREMENTAL_CAPTURE_FLAG"]:
        step_id = step_id + 100
        data_flow_list.append(drop_transient_table(object_data, step_id))
    else:
        pass
    return data_flow_list


def construct_pipeline_json(session, process_name: str, object_data: dict) -> json:
    """
    Method to construct pipeline json .

    Args:
    session (Snowpark Session): snowpark session object
    process_name (str): name of the process for which metadata generation is triggered
    object_data (dict): dictionary of object attributes data

    Returns:
    json: json containing steps for data process

    """
    # Construct data context
    query_list = []
    if object_data.get("ROLE"):
        query_list.append(f"use role {object_data['ROLE']}")
    query_list.extend(
        [
            f"use warehouse {object_data['COMPUTE_WH_NAME']}",
            f"use database {object_data['DATABASE_NAME']}",
            f"use schema {object_data['SCHEMA_NAME']}",
        ]
    )

    data_context = {"query_list": query_list}
    # Construct data flow json
    data_flow = construct_data_flow(session, process_name, object_data)
    logger.info(f"Data Flow JSON successfully created for process '{process_name}'")

    # Get data from email attributes table if configured in object_attr
    email_intg_name, email_addr_list = None, None
    if object_data.get("EMAIL_INTG_NAME"):
        email_attr_df = (
            session.table("email_attr")
            .where(
                f"lower(email_intg_name) = lower('{object_data['EMAIL_INTG_NAME']}')"
            )
            .first()
        )
        if email_attr_df:
            email_attr = email_attr_df.as_dict()
        else:
            raise ValueError(
                f"Email integration name {object_data['EMAIL_INTG_NAME']} is not found in email attributes"
            )

        email_intg_name = email_attr["EMAIL_INTG_NAME"]
        email_addr_list = json.loads(email_attr["EMAIL_ADDR_LIST"])
        logger.info(f"Retrieved email attributes for process '{process_name}'")
    else:
        logger.info(f"No email integration configured for process '{process_name}'")

    # Construct pipeline json
    pipeline_metadata = {
        "root": {
            "pipeline_name": object_data["PROCESS_NAME"],
            "desc": object_data["PROCESS_DESC"]
            if object_data["PROCESS_DESC"]
            else None,
            "email_intg_name": email_intg_name,
            "email_addr_list": email_addr_list,
            "data_context": data_context,
            "data_flow": data_flow,
        }
    }
    pipeline_metadata_json = json.dumps(pipeline_metadata)

    return pipeline_metadata_json
