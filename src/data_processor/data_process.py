"""
Module for executing ETL pipelines defined in a JSON configuration file.

This module provides a function executor that reads a JSON object containing
a description of the ETL pipeline to execute, and then executes each function in the pipeline
in the order specified. The JSON object should contain a root object called `PIPELINE_CONFIG`
that defines the various stages and steps of the pipeline, as well as any runtime parameters
or other configuration settings.

The module also provides utility functions for fetching the pipeline configuration from a
Snowflake metadata database, checking if the pipeline is in restart mode, and sending email
notifications upon successful completion or failure of the pipeline.

To execute a pipeline, call the `function_executor` function with the following arguments:
    - `sf_s`: A Snowpark session object.
    - `process_name`: The name of the pipeline process to execute.
    - `conf`: A dictionary of configurations containing the Snowflake credentials and other settings.
    - `runtime_params`: A dictionary containing any runtime parameters that should be passed to the pipeline.
    - `restart_params`: A dictionary containing any restart parameters
    (i.e. `restart_run_id` and `restart_step_id`) if applicable.
"""
import json
import sys
from datetime import datetime
import traceback
from snowflake.snowpark import Session
import src.data_processor.core.functions as library_function
from src.utils.logr import (
    sf_logger,
    set_logger,
    get_logs_list,
    set_framework_logger,
    fw_logger,
)
from src.utils.utils import (
    get_config,
    update_job_status,
    print_stat_send_email,
    get_args,
    create_session,
    log_str_size_check,
)


def etl_func(sf_s, df_dict, restart_step_id, func_dict):
    """Execute an ETL function specified in a dataflow dictionary.

    This function extracts arguments from the `func_dict` dictionary and calls
    the corresponding ETL function from the`library_function` module with those
    arguments. If the ETL function returns a dictionary, it updates the `df_dict`
    dictionary with the new key-value pairs.

    Parameters:
    sf_s (Session): A Snowpark session.
    df_dict (dict): A dictionary containing data and metadata for the ETL process.
    restart_step_id (str): The ID of the last completed step in a restart mode.
    Steps with lower IDs will be skipped.
    func_dict (dict): A dictionary containing the configuration for the ETL function
    to be executed.

    Returns:
    None
    """

    # Check step ID with restart_step_id to handle the restart from specific step.
    # In normal start restart_step_id defaults to '000'
    if int(restart_step_id) <= int(func_dict["step_id"]):
        sf_logger.info(f"Function dictionary: {func_dict}")
        func_args = {k: v for k, v in func_dict.items() if k != "func"}
        func = getattr(library_function, func_dict["func"])
        func_output = func(sf_s, func_args, df_dict)

        # update df_dict if output is dataframe
        if func_output is not None:
            df_dict.update(func_output)
        else:
            sf_logger.info("No output to update dataframe dictionary")
    else:
        sf_logger.info(
            f"Restart mode. "
            f"Skipping step ID `{func_dict['step_id']}` and function `{func_dict['func']}` : {func_dict['desc']}"
        )


def check_restart_cond(process_name, restart_params):
    """
    Check if a pipeline is in restart mode and return a dictionary of restart params

    Args:
        - process_name (str): Name of the process for which pipeline has been triggered.
        - restart_params (dict): Dictionary containing restart parameters

    Returns:
          dict: dictionary of restart params updated with restart flag

    """

    # Check if pipeline is started in `restart` mode
    if restart_params.get("restart_run_id"):
        sf_logger.info(
            f"Pipeline {process_name} has been started in restart mode. "
            f"Failed run_id is {restart_params['restart_run_id']}"
        )
        restart_params.update({"restart_flag": True})

        if restart_params.get("restart_step_id"):
            sf_logger.info(
                f"Pipeline will start executing from step id {restart_params['restart_step_id']}"
            )
    else:
        sf_logger.info(f"pipeline `{process_name}` has been started.")
    return restart_params


def get_pipeline(sf_session, process_name, meta_db, meta_schema):
    """Fetch the pipeline configuration for a given process name from the Snowflake
    metadata database.
    Parameters:
    sf_session (Session): A Snowflake session object.
    process_name (str): The name of the pipeline process to fetch the configuration for.
    meta_db (str): The name of the metadata database containing the `pipeline` table.
    meta_schema (str): The name of the metadata schema containing the `pipeline` table.

    Returns:
    dict: A dictionary representing the pipeline configuration for the specified
    process name. The dictionary contains nested dictionaries and lists corresponding
    to the various stages and steps of the pipeline, as well as any runtime parameters
    or other configuration settings specified in the `pipeline` table."""

    pipeline_rec = (
        sf_session.table(f"{meta_db}.{meta_schema}.pipeline")
        .where(f"process_name = '{process_name}'")
        .first()
    )
    if pipeline_rec:
        return pipeline_rec.as_dict()["PIPELINE_CONFIG"]

    raise ValueError(f"Pipeline `{process_name}` does not exist")


def validate_threshold_update_job_status(df_dict, job_status_args):
    """
    Validates the success/failure status of a data processing job based on a configured threshold percentage
    and updates the job status accordingly.

    Args:
        - df_dict (dict):
        - job_status_args (dict):
    """
    # successful_record_count = int(df_dict.get("total_records_processed", 0))
    total_record_count = int(df_dict.get("total_records_processed", 0))
    failed_record_count = int(df_dict.get("failed_record_count", 0))
    successful_record_count = total_record_count - failed_record_count

    actual_threshold_percentage = 0
    configured_threshold_percentage = 0

    if df_dict.get("threshold_percentage") and total_record_count > 0:
        configured_threshold_percentage = int(df_dict["threshold_percentage"])
        actual_threshold_percentage = (
            successful_record_count / total_record_count
        ) * 100

    if actual_threshold_percentage >= configured_threshold_percentage:
        job_status = "SUCCEEDED"
        job_status_args.update(
            {
                "successful_record_count": successful_record_count,
                "failed_record_count": failed_record_count,
            }
        )
    else:
        job_status = "FAILED"
        sf_logger.error(
            f"Failure in threshold evaluation "
            f"configured threshold percentage: {configured_threshold_percentage} "
            f"actual threshold percentage: {actual_threshold_percentage}"
        )
        job_status_args.update(
            {
                "successful_record_count": successful_record_count,
                "failed_record_count": failed_record_count,
                "failure_reason": "Threshold percentage is not met",
            }
        )

    update_job_status(
        pipeline_status=job_status,
        log_msg_str=log_str_size_check(get_logs_list()),
        **job_status_args,
    )

    return job_status, job_status_args


def function_executor(
    sf_s: Session,
    process_name: str,
    conf: dict,
    runtime_params: dict,
    restart_params: dict,
):
    """
    Executes functions specified in pipeline config for the process `process_name`

    Parameters:
    ss (Session): A Snowpark session
    process_name (str): The name of the process to be executed
    conf (dict): A dictionary of configurations
    restart_run_id (str, optional): A unique identifier for a previous run, used for
    restarting a run

    Returns:
    None
    """
    # Generate unique run id
    df_dict = {
        "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "run_id": int(datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3]),
    }
    fw_logger.info(
        "Starting function executor for process name: %s and run ID: %s",
        process_name,
        df_dict["run_id"],
    )

    sf_logger.info(f"Process name: {process_name}")
    sf_logger.info(f"run ID: {df_dict['run_id']}")

    restart_run_id: int = restart_params.get("restart_run_id")
    restart_step_id: int = restart_params.get("restart_step_id")

    job_status_args = {
        "ss": sf_s,
        "process_name": process_name,
        "run_id": df_dict["run_id"],
        "restart_run_id": restart_run_id,
        "database": conf["snowflake"]["database"],
        "schema": conf["snowflake"]["schema"],
        "meta_wh": conf["snowflake"]["warehouse"],
    }
    pipeline = ""
    pipeline_dict = {}
    job_status = ""
    df_dict.update(conf)
    df_dict.update({"runtime_params": runtime_params})

    try:
        pipeline = get_pipeline(
            sf_s,
            process_name,
            conf["snowflake"]["database"],
            conf["snowflake"]["schema"],
        )
        pipeline_dict = json.loads(pipeline)

        sf_logger.info(f"Pipeline config: {pipeline_dict}")

        # Insert record into the job status table
        update_job_status(
            action="insert",
            pipeline_config=json.dumps(pipeline_dict).replace("'", "''"),
            **job_status_args,
        )

        # Update job status args for 'update' status
        job_status_args.update({"action": "update", "pipeline_config": pipeline})

        # Check if pipeline is started in `restart` mode and update the dictionary
        df_dict.update(check_restart_cond(process_name, restart_params))

        # Get dataflow from pipeline config
        dataflow = pipeline_dict["root"]["data_flow"]

        # Set Data context for Data processing
        library_function.ddl(
            ss=sf_s, func_args=pipeline_dict.get("root").get("data_context")
        )

        # Loop through functions in dataflow and execute them
        for func_dict in dataflow:
            if df_dict.get("stream_has_data", True):
                etl_func(sf_s, df_dict, restart_step_id, func_dict)
            else:
                fw_logger.info(
                    f"Skipping step ID `{func_dict['step_id']}` and "
                    f"function `{func_dict['func']}` : {func_dict['desc']}"
                )

        sf_s.sql(f'use role {conf["snowflake"]["role"]}').collect()
        job_status, job_status_args = validate_threshold_update_job_status(
            df_dict, job_status_args
        )
    except Exception as ex_data:
        sf_logger.error(traceback.format_exc())
        sf_logger.error(f"pipeline `{process_name}` has been failed with {ex_data}.")
        sf_s.sql(f'use role {conf["snowflake"]["role"]}').collect()
        # Update 'FAILED' status in job status table
        job_status = "FAILED"
        if pipeline:
            update_job_status(
                pipeline_status=job_status,
                log_msg_str=log_str_size_check(get_logs_list()),
                **job_status_args,
            )

            sf_logger.error("Please check job_status_history table for more details")
    finally:
        print_stat_send_email(
            sf_s,
            {
                "status": job_status,
                "process_name": process_name,
                "start_time": df_dict.get("start_time"),
                "email_intg_name": pipeline_dict.get("root", {}).get("email_intg_name"),
                "email_addr_list": pipeline_dict.get("root", {}).get("email_addr_list"),
                "total_records_processed": df_dict.get("total_records_processed", "0"),
                "failed_record_count": df_dict.get("failed_record_count", "0"),
                "run_id": df_dict["run_id"],
                "failure_reason": job_status_args.get("failure_reason"),
                "email_notification_subscription": conf.get("email_notification_subscription", "both")
            },
        )

        if job_status == "SUCCEEDED":
            sf_logger.info(
                f"pipeline `{process_name}` has been completed successfully."
            )
        else:
            sf_logger.error(f"pipeline `{process_name}` has been failed.")
            sys.exit(1)

        sf_s.close()

    fw_logger.info(f"Exiting function executor for process name: {process_name}")


# Entry point of the script
if __name__ == "__main__":
    # Parse Commandline arguments
    params = get_args()

    # Setting log level for framework
    set_framework_logger(params.log_level.upper())

    fw_logger.info("Starting Data Processing component")
    fw_logger.info(f"Framework log level is set to `{params.log_level.upper()}`")

    # Read configuration file (from S3)
    fw_logger.info(f"Reading configuration file `{params.config_file}`")
    pipeline_config = get_config(params.config_file)["config"]

    # Set logging level
    fw_logger.info(f"Setting Snowpark log level to `{pipeline_config['log_level']}`")
    set_logger(pipeline_config["log_level"])

    # Create Snowpark session
    fw_logger.info("Calling Snowpark Session creation")

    if "snowflake" in pipeline_config:
        session = create_session(pipeline_config["snowflake"])
    else:
        raise ValueError(
            f"Snowflake configuration not found in config file {params.config_file} "
        )

    # Check runtime params passed
    rt_params = json.loads(params.runtime_params) if params.runtime_params else {}

    # Restart arguments
    restart_args = {
        "restart_run_id": params.restart_run_id,
        "restart_step_id": params.restart_step_id,
        "status_code": params.status_code,
    }

    # Call function executor with the session, process name, configuration
    # and restart run ID, restart step ID in case of restart

    fw_logger.info("Calling function executor")
    function_executor(
        sf_s=session,
        process_name=params.process_name,
        conf=pipeline_config,
        runtime_params=rt_params,
        restart_params=restart_args,
    )

    fw_logger.info("End of the Data Processing component")
