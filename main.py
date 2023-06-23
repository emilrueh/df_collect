from src.settings_scripts import load_settings

# event collection
from src.eventbrite_scripts import get_all_events_info, create_eventbrite_object
from src.meetup_scripts import scrape_meetup

# ai summary
from src.openai_scripts import openai_loop_over_column_and_add

# data manipulation
from src.data_scripts import (
    create_df,
    flatten_data,
    delete_duplicates_add_keywords,
    manipulate_csv_data,
)

# send to db
from src.xano_scripts import send_data_to_xano

import time
import traceback
import sys


# main function calling functions
def main():
    try:
        # TIMING
        start_time = time.time()
        print(f"Start: {start_time}")

        # SETTINGS
        settings = load_settings(
            r"C:\Users\emilr\Code\PythonProjects\openaiapi\meetupsummary\data\settings_test.json"
        )
        # EVENTBRITE
        eventbrite_return = get_all_events_info(
            eventbrite=create_eventbrite_object(
                private_token=settings["EVENTBRITE_PRIVATE_TOKEN"]
            ),
            keywords=settings["KEYWORDS"],
            search_url=settings["EVENTBRITE_URL"],
            location=settings["EVENT_LOCATION"],
            max_pages=settings["EB_PAGINATION"],
        )

        # MEETUP
        meetup_return = scrape_meetup(
            url=settings["MEETUP_URL"], keywords=settings["KEYWORDS"], event_entries=100
        )

        # JSON
        flattened_meetup = flatten_data(meetup_return)
        flattened_eventbrite = flatten_data(eventbrite_return)
        full_events_return = flattened_eventbrite + flattened_meetup

        # CSV
        df = create_df(full_events_return)
        df = delete_duplicates_add_keywords(
            data=df,
            columns_to_compare=["Name", "Long_Description", "Date", "Price"],
        )
        df = manipulate_csv_data(
            input_df=df,
            operations=settings["CSV_OPERATIONS"],
            output_filepath=settings["PATH_TO_CSV"],
            file_path=None,
        )

        # OPENAI
        df = openai_loop_over_column_and_add(
            api_key=settings["OPENAI_API_KEY"],
            prompt=settings["AI_PROMPT"],
            data=df,
            column_for_input="Long_Description",
            column_for_output="Summary",
            path_to_file=r"C:\Users\emilr\Code\PythonProjects\openaiapi\meetupsummary\src\notebooks\full_events_summarized.csv",
            char_max=170,
            char_min=48,
            to_remove=['"'],
        )

        # XANO
        xano_response = send_data_to_xano(
            data=df,
            api_key=settings["XANO_API_KEY"],
            image_endpoint_url=settings["XANO_ENDPOINT_IMAGE"],
            update_endpoint_url=settings["XANO_ENDPOINT_POST"],
            get_all_endpoint_url=settings["XANO_ENDPOINT_GET_ALL"],
        )

        # TIMING
        end_time = time.time()
        print(f"End: {end_time}")

        execution_time = end_time - start_time

        print(f"The script executed in {execution_time} seconds")
    # # #

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback_details = traceback.extract_tb(exc_traceback)

        fname = traceback_details[-1].filename
        line_number = traceback_details[-1].lineno
        exc_type_name = exc_type.__name__

        print(
            f"\nERROR\n------\n{exc_type_name} at line: {line_number}\n{fname}\n{e}\n------\n"
        )
        view_traceback = input("Do you want to see the full error message? (y/n) ")
        if view_traceback == "y".casefold():
            print(f"\n{traceback_details}\n{e}\n")


# Run the main function
if __name__ == "__main__":
    main()
