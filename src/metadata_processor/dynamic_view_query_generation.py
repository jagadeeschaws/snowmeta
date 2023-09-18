"""
Script: dynamic_view_query_generation.py
Purpose: This module generates the dynamic view query using the view configuration provided
        in query_view_config, query_order_config & query_template_config tables
"""
import json
import re
from meta_logr import sf_logger as logger


# resolve runtime config
def resolve_runtime_config(query: str, parameters: str, query_template_df) -> str:
    """
    Resolve query if query is configured with query template, and resolve runtime parameters.

    Args:
        query (str): query string
        parameters (str): runtime parameters as string
        query_template_df (dataframe): dataframe of query templates

    Returns:
        str: resolved query string

    Raises:
        KeyError: if runtime parameter is not found in parameter dictionary

    """
    # Resolve query if query is configured with query template
    resolved_template_query = re.sub(
        r"t{{(.+?)}}",
        lambda x: str(get_query_template(query_template_df, x.group(1))),
        query,
    )

    # Resolve runtime parameters
    if parameters:
        param_dict = json.loads(parameters)

        try:
            resolved_query = re.sub(
                r"{{(.+?)}}",
                lambda x: str(param_dict[x.group(1)]),
                resolved_template_query,
            )
        except KeyError as e:
            raise KeyError(f"parameter '{e.args[0]}' not found in param_dict") from e

    else:
        query_params = re.findall("\\{\\{(.+?)\\}\\}", resolved_template_query)
        if not query_params:
            resolved_query = query
        else:
            raise KeyError(
                f"Undefined parameters {query_params} detected. "
                f"Please provide all the required parameters in query : {resolved_template_query}."
            )

    return resolved_query


# Read query template dataframe
def get_query_template(query_template_df, tmp_name: str) -> str:
    """
    Get the query template for the given name.

    Args:
        query_template_df (dataframe): dataframe of query templates
        tmp_name (str): name of query template

    Returns:
        str: query template

    """
    try:
        query_template_res = (
            query_template_df.filter(f"query_template_name = '{tmp_name}'")
            .first()
            .asDict()
        )
        logger.info(f"Retrieved query template for {tmp_name}")
    except Exception as exc:
        raise ValueError(
            f"Configured template {tmp_name} is not found in query template config table"
        ) from exc
    return query_template_res["QUERY_TEMPLATE"]


def get_query_template_config(
    query_template_df, query_view_config: dict, view_name: str
) -> str:
    """
    Resolve the parameters for query or qeury template.

    Args:
        query_template_df (dataframe): dataframe of query templates
        query_view_config (dict): dictionary of query_view_config table
        view_name (str): name of the view to be created

    Returns:
        str: resolved query config

    """
    if query_view_config["QUERY_TEMPLATE_NAME"]:
        logger.debug("Retrieving query template")
        query_tmpl = get_query_template(
            query_template_df,
            query_view_config["QUERY_TEMPLATE_NAME"],
        )
        abs_result_config_query = resolve_runtime_config(
            query_tmpl, query_view_config["PARAMETERS"], query_template_df
        )
    elif query_view_config["QUERY"]:
        abs_result_config_query = resolve_runtime_config(
            query_view_config["QUERY"],
            query_view_config["PARAMETERS"],
            query_template_df,
        )
    else:
        raise ValueError(
            f"Either query or query template has to be provided in query view config table for the view {view_name} "
        )

    return abs_result_config_query


def construct_cte(query_order_result: list, query_template_df) -> list:
    """
    Resolve the parameters for query or qeury template.

    Args:
        query_order_result (list): list of row objects containing data from query_order_config
        query_template_df (dataframe): dataframe of query templates

    Returns:
        list: resolved query config

    """
    query_expr_list = []
    logger.info("Constructing CTE expression")
    for q_ord in query_order_result:
        if q_ord["QUERY"]:
            abs_query = resolve_runtime_config(
                q_ord["QUERY"], q_ord["PARAMETERS"], query_template_df
            )
            query_expr_list.append(f"{q_ord['TEMP_TABLE']} AS ({abs_query})")
        elif q_ord["QUERY_TEMPLATE_NAME"]:
            abs_query_from_tmpl = resolve_runtime_config(
                get_query_template(query_template_df, q_ord["QUERY_TEMPLATE_NAME"]),
                q_ord["PARAMETERS"],
                query_template_df,
            )
            query_expr_list.append(f"{q_ord['TEMP_TABLE']} AS ({abs_query_from_tmpl})")
        else:
            pass
    return query_expr_list


def create_dynamic_view_query(
    session,
    view_name: str,
    primary_table: str,
    transient_stg_table: str,
    incremental_flag: bool,
) -> str:
    """
    Generate dynamic query views.

    Args:
        session (Snowpark Session): snowpark session object
        view_name (str): name of the view to be created
        primary_table (str): name of driving table for view

    Returns:
       str: resolved final query
    """
    logger.info(f"Generating dynamic query view {view_name}")
    # Read query result config
    logger.debug("Retrieving query view configuration")

    query_view_result = (
        session.table("query_view_config")
        .where(f"lower(view_name) = lower('{view_name}')")
        .collect()
    )

    if query_view_result:
        query_view_config = query_view_result[0]
    else:
        raise ValueError(
            f"Configured view name {view_name} is not found in query_view_config"
        )

    # Read query template config
    logger.debug("Retrieving query template configuration")
    query_template_df = session.table("query_template_config").cache_result()
    abs_result_config_query = get_query_template_config(
        query_template_df, query_view_config, view_name
    )

    # Get query_order_config
    logger.debug("Retrieving query order configuration.")
    query_order = (
        f"SELECT * FROM query_order_config where lower(view_name) = lower('{view_name}') order by "
        f"EXECUTION_SEQUENCE asc"
    )
    query_order_result = session.sql(query_order).collect()

    # If query order is not empty then construct the CTE
    if query_order_result:
        cte_query = ",".join(construct_cte(query_order_result, query_template_df))
        dynamic_view_query = (
            f"create or replace temporary view {view_name} "
            f"as with {cte_query} {abs_result_config_query}"
        )
    else:
        logger.warning(f"Query order is empty for view {view_name}")
        dynamic_view_query = (
            f"create or replace temporary view {view_name} as {abs_result_config_query}"
        )

    # Replace the primary table name with transient that captures the incremental data.
    if primary_table and incremental_flag:
        logger.info("Change primary table into staging table")
        final_view_query = re.sub(
            r"(?:(?<=FROM\s)|(?<=,)|(?<=\s))((?:[\w.]*)?\b" + primary_table + r"\b)",
            f"{transient_stg_table}",
            dynamic_view_query,
            flags=re.IGNORECASE,
        )
    else:
        final_view_query = dynamic_view_query

    logger.info(f"Query generated for view {view_name}: {final_view_query}")
    return final_view_query
