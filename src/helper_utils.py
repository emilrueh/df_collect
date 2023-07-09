import logging

# from main import logger_name

# logger = logging.getLogger(logger_name)

from datetime import datetime
import os

import subprocess

from dotenv import load_dotenv
import json

# GLOBAL VARIABLES


# LOGGING
main_logger_name = "main_logger"
notebook_logger_name = "nb_logger"
test_logger_name = "test_logger"
__name__logger_name = __name__


def config_logger(
    name: str = "diary_logger:",
    lvl_console: str = "DEBUG",
    lvl_file: str = None,
    lvl_root: str = "INFO",
    fmt_console="%(name)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s",
    fmt_file="%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s",
    fmt_date: str = "%Y-%m-%d %H:%M:%S:Ms",
    file_name: str = "runtime",
    file_timestamp: str = "%Y%m%d-%H%M%S",
    prints: bool = False,
) -> object:
    logger = logging.getLogger(name)

    base_values_dict = {
        "name": name if name == "diary_logger" else None,
        "lvl_console": lvl_console if lvl_console == "DEBUG" else None,
        "lvl_file": lvl_file if lvl_file is None else None,
        "lvl_root": lvl_root if lvl_root == "INFO" else None,
        "fmt_console": fmt_console
        if fmt_console
        == "%(name)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s"
        else None,
        "fmt_file": fmt_file
        if fmt_file
        == "%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s"
        else None,
        "fmt_date": fmt_date if fmt_date == "%Y-%m-%d %H:%M:%S:Ms" else None,
        "file_name": file_name if file_name == "runtime" else None,
        "file_timestamp": file_timestamp if file_timestamp == "%Y%m%d-%H%M%S" else None,
        "prints": prints if prints == False else None,
    }

    base_values = {key for key, value in base_values_dict.items() if value is not None}

    if base_values:
        print(f"Starting with the base settings for {', '.join(base_values)}.")

    if file_timestamp:
        file_timestamp = datetime.now().strftime(file_timestamp)

    level_options = ["debug", "info", "warning", "exception", "error", "critical"]

    if lvl_file:
        if not os.path.exists("logs"):
            os.makedirs("logs")

        f_handler = logging.FileHandler(
            filename=os.path.join("logs", f"{file_name}_{file_timestamp}.log"), mode="a"
        )
        f_format = logging.Formatter(
            fmt=fmt_file,
            datefmt=fmt_date,
        )
        if lvl_file in level_options:
            lvl_file = logging.getLevelName(lvl_file.upper())
        else:
            default_lvl_file = "DEBUG"
            print(f"Set the file level to {default_lvl_file}.")
            lvl_file = logging.getLevelName(default_lvl_file.upper())

        f_handler.setLevel(lvl_file)
        f_handler.setFormatter(f_format)
        logger.addHandler(f_handler)

    if lvl_console:
        c_handler = logging.StreamHandler()
        c_format = logging.Formatter(
            fmt=fmt_console,
            datefmt=fmt_date,
        )
        if lvl_console in level_options:
            lvl_console = logging.getLevelName(lvl_console.upper())
        else:
            default_lvl_console = "DEBUG"
            print(f"Set the console level to {default_lvl_console}.")
            lvl_console = logging.getLevelName(default_lvl_console.upper())

        c_handler.setLevel(lvl_console)
        c_handler.setFormatter(c_format)
        logger.addHandler(c_handler)

    if lvl_root in level_options:
        logger.setLevel(lvl_root.upper())
    else:
        default_lvl_root = "DEBUG"
        print(f"Set the root level to {default_lvl_root}.")
        logger.setLevel(default_lvl_root.upper())

    if prints:  # can get improved
        if lvl_console and lvl_file:
            print("Logging to console and file...")
        elif lvl_console:
            print("Logging to console...")
        elif lvl_file:
            print("Logging to file...")

    return logger


# SETTINGS
def load_settings(settings_path, load_secrets=False):
    # Initialize an empty dictionary for secrets
    secrets_dict = {}

    # Load .env variables if load_secrets is True
    if load_secrets:
        load_dotenv()

        secrets_dict = {
            "AIRTABLE_API_TOKEN": os.getenv("AIRTABLE_API_TOKEN"),
            "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY"),
            "EVENTBRITE_PRIVATE_TOKEN": os.getenv("EVENTBRITE_PRIVATE_TOKEN"),
            "XANO_API_KEY": os.getenv("XANO_API_KEY"),
            "XANO_ENDPOINT_POST": os.getenv("XANO_ENDPOINT_POST"),
            "XANO_ENDPOINT_IMAGE": os.getenv("XANO_ENDPOINT_IMAGE"),
            "XANO_ENDPOINT_GET_ALL": os.getenv("XANO_ENDPOINT_GET_ALL"),
            "XANO_ENDPOINT_EDIT": os.getenv("XANO_ENDPOINT_EDIT"),
            "XANO_ENDPOINT_DELETE": os.getenv("XANO_ENDPOINT_DELETE"),
            "XANO_TABLE_NAME": os.getenv("XANO_TABLE_NAME"),
        }

    # Check if the settings file exists
    if not os.path.exists(settings_path):
        raise FileNotFoundError(f"Settings file not found: {settings_path}")

    # Load the settings dictionary from the .json file
    with open(settings_path, "r") as fp:
        settings_dict = json.load(fp)

    # Merge the API keys and other settings
    settings_dict.update(secrets_dict)

    return settings_dict


# GITHUB
def get_git_tree(repo_path="."):
    def create_tree_string(tree, indent=""):
        tree_string = ""
        for name, node in tree.items():
            tree_string += f"{indent}{name}\n"
            if isinstance(node, dict):
                tree_string += create_tree_string(node, indent + "    ")
        return tree_string

    # Get list of files in repository
    result = subprocess.run(
        ["git", "ls-files"], capture_output=True, cwd=repo_path, text=True
    )
    files = result.stdout.split("\n")

    # Build and print directory tree
    tree = {}
    for file in files:
        path = file.split("/")
        node = tree
        for part in path:
            node = node.setdefault(part, {})
    return create_tree_string(tree)
