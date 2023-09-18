"""
Script: meta_logr.py
Purpose: This is the logger module for metadata process
"""
import logging
from io import StringIO
from colorlog import ColoredFormatter


logs_list = StringIO()
# Logger instance for Framework
fw_logger = logging.getLogger("data_processing")
# Logger instance for Snowpark
sf_logger = fw_logger.getChild("snowflake.snowpark")


def set_logger(log_lvl: str):
    """
    This function sets up the logger and takes in one argument

    Args:
    log_lvl (str) : The desired log level (e.g. "DEBUG", "INFO", etc)
    """
    # Get the log level constant corresponding to the log level name
    log_level = logging.getLevelName(log_lvl)
    # Set the log level for the logger
    sf_logger.setLevel(log_level)
    # Create a stream handler for the logs_list variable
    ch = logging.StreamHandler(logs_list)
    ch.setLevel(log_level)
    # Set the log message formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(threadName)s %(filename)s:%(lineno)d - %(funcName)s() - %("
        "levelname)s - %(message)s"
    )
    ch.setFormatter(formatter)
    # Add the stream handler to the logger
    sf_logger.addHandler(ch)


def set_framework_logger(log_lvl: str):
    """
    This function sets up the framework logger

    Args:
    log_lvl (str) : The desired log level (e.g. "DEBUG", "INFO", etc)
    """
    log_level = logging.getLevelName(log_lvl)
    fw_logger.setLevel(log_level)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_lvl)
    console_handler.setFormatter(
        ColoredFormatter(
            "%(log_color)s%(asctime)s "
            "- %(threadName)s %(filename)s:%(lineno)d "
            "- %(funcName)s() "
            "- %(levelname)s - %(message)s",
            datefmt=None,
            style="%",
        )
    )
    fw_logger.addHandler(console_handler)


def get_logs_list():
    """
    This function returns the logs captured in the logs_list variable
    logs_list : io.StringIO : An instance of io.StringIO that contains the logs
    """
    return logs_list.getvalue()


def clear_log():
    """
    This function clears the logs captured in the logs_list variable
    logs_list : io.StringIO : An instance of io.StringIO that contains the logs
    """
    return logs_list.truncate(0), logs_list.seek(0)
