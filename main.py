from src.helper_utils import config_logger, main_logger_name

logger_name = main_logger_name

logger = config_logger(
    logger_name,
    lvl_console="info",
    lvl_file="debug",
    lvl_root="debug",
    fmt_console="\n%(asctime)s - %(name)s - %(levelname)s - %(module)s @line %(lineno)3d\n%(message)s",
    fmt_file="%(asctime)s - %(name)s - %(levelname)s - %(module)s @line %(lineno)3d ---> %(message)s",
    fmt_date="%d.%m.%Y %H:%M:%S",
    file_name="runtime",
    file_timestamp="%d%m%Y-%H%M%S",
    prints=True,
)

import time
import traceback
import sys
from termcolor import colored
import os

from src.helper_utils import load_settings

# event collection
from src.eventbrite_scripts import get_all_events_info, create_eventbrite_object
from src.meetup_scripts import scrape_meetup

# ai summary
from src.openai_scripts import openai_loop_over_column_and_add

# data manipulation
from src.data_utils import (
    create_df,
    flatten_data,
    delete_duplicates_add_keywords,
    manipulate_csv_data,
    map_keywords_to_categories,
    load_from_csv,
    json_read,
)

# send to db
from src.xano_scripts import send_data_to_xano


def main():
    try:
        start_time = time.time()
        logger.info(f"Start time: {start_time}")

        # SETTINGS
        settings_path = r"C:\Users\emilr\Code\PythonProjects\openaiapi\meetupsummary\data\settings.json"
        logger.info(f"Loading settings from: {os.path.basename(settings_path)}")
        settings = load_settings(settings_path, load_secrets=True)

        # SCRAPER
        # # EVENTBRITE
        eventbrite_return = get_all_events_info(
            eventbrite=create_eventbrite_object(
                private_token=settings["EVENTBRITE_PRIVATE_TOKEN"]
            ),
            keywords=settings["KEYWORDS"],
            search_url=settings["EVENTBRITE_URL"],
            location=settings["EVENT_LOCATION"],
            max_pages=settings["EB_PAGINATION"],
        )

        logger.info(
            colored(f'Path to the output csv: {settings["PATH_TO_CSV"]}', "cyan")
        )

        # # MEETUP
        meetup_return = scrape_meetup(
            url=settings["MEETUP_URL"],
            keywords=settings["KEYWORDS"],
            event_entries=settings["MU_PAGINATION"],
        )

        # DATA
        # # JSON
        flattened_meetup = flatten_data(meetup_return)
        flattened_eventbrite = flatten_data(eventbrite_return)
        full_events_return = flattened_eventbrite + flattened_meetup

        # # CSV
        # creating df from json
        df = create_df(full_events_return)

        # # # deleting duplicates and appending keywords
        df = delete_duplicates_add_keywords(
            data=df,
            columns_to_compare=["Name", "Long_Description", "Date", "Price"],
        )
        # # # df manipulation operations
        df = manipulate_csv_data(
            input_df=df,
            operations=settings["CSV_OPERATIONS"],
            output_filepath=settings["PATH_TO_CSV"],
            file_path=None,
        )
        # # # creating catgories from keywords
        df["Category"] = df["Keyword"].apply(
            lambda x: map_keywords_to_categories(x, settings["CATEGORIES"])
        )
        # # # creating archived column
        df["Archived"] = False

        # APIs
        # # OPENAI
        df = openai_loop_over_column_and_add(
            api_key=settings["OPENAI_API_KEY"],
            prompt=settings["AI_PROMPT"],
            data=df,
            column_for_input="Long_Description",
            column_for_output="Summary",
            path_to_file=settings["PATH_TO_CSV"],
            char_max=170,
            char_min=48,
            to_remove=['"'],
        )

        # # XANO
        xano_response = send_data_to_xano(
            data=df,
            api_key=settings["XANO_API_KEY"],
            image_endpoint_url=settings["XANO_ENDPOINT_IMAGE"],
            send_endpoint_url=settings["XANO_ENDPOINT_POST"],
            check_endpoint_url=settings["XANO_ENDPOINT_GET_ALL"],
            edit_endpoint_url=settings["XANO_ENDPOINT_EDIT"],
            delete_endpoint_url=settings["XANO_ENDPOINT_DELETE"],
            table_name=settings["XANO_TABLE_NAME"],
        )

        end_time = time.time()
        logger.info(f"End: {end_time}")
        execution_time = round(end_time - start_time, 2)
        logger.info(
            colored(f"The script executed in {execution_time / 60} minutes.", "cyan")
        )

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback_details = traceback.extract_tb(exc_traceback)

        fname = traceback_details[-1].filename
        line_number = traceback_details[-1].lineno
        exc_type_name = exc_type.__name__

        logger.critical(f"{exc_type_name}: {e}")

        view_traceback = input("View full error? (y/n) ")
        if view_traceback.lower() == "y":
            error_traceback = (
                f"Error traceback: {traceback_details}\nError message: {e}"
            )
            logger.critical(error_traceback)


# Run the main function
if __name__ == "__main__":
    main()
