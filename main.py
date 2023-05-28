from src.scraper_scripts import scrape_website
from src.file_scripts import (
    save_to_csv,
    load_from_csv,
    delete_csv_duplicates,
    json_read,
)
from src.airtable_scripts import csv_to_airtable
from src.openai_scripts import openai_loop_over_column_and_add
from src.settings_scripts import set_scraper_settings

import traceback
import sys


# main function calling functions
def main():
    # settings["AIRTABLE_API_TOKEN"]
    # settings["AIRTABLE_API_URL"]
    # settings["OPENAI_API_KEY"]
    # settings["URL_TO_SCRAPE"]
    # settings["PATH_TO_CSV"]
    # settings["KEYWORDS"]
    # settings["NUMBER_OF_EVENTS_PER_KEYWORD"]
    # settings["AI_PROMPT"]

    try:
        # SETTINGS
        settings = set_scraper_settings(
            ai_prompt="Summarize the following event description in one short and concise sentence. Do not include specific information, only focus on the core idea and have an excited tone to it. Do not write any headings. The sentence can be at maximum 125 characters long:",
            keywords=[
                "founder",
                "invest",
                "funding",
                "entrepreneur",
                "venture capital",
                "startup",
                "marketing",
                "sales",
                "network",
                "web3",
                "crypto",
                "ai",
                "tech",
                "product",
                "code",
                "real estate",
                "conference",
            ],
            number_of_events_per_keyword=100,
        )

        # # scraper scripts
        # scraper_return = scrape_website(
        #     url=settings["URL_TO_SCRAPE"],
        #     keywords=settings["KEYWORDS"],
        #     event_entries=settings["NUMBER_OF_EVENTS_PER_KEYWORD"],
        # )

        # json scripts
        keyword_results_dict = json_read(
            json_filename="scraper_backup.json",
        )

        # csv scripts
        save_to_csv(data=keyword_results_dict, filename=settings["PATH_TO_CSV"])
        load_from_csv(filename=settings["PATH_TO_CSV"])

        # checking for duplicates
        delete_csv_duplicates(
            file_path=settings["PATH_TO_CSV"],
            columns_to_compare=["Name", "Date", "Time", "Location"],
        )

        # openai scripts
        openai_loop_over_column_and_add(
            api_key=settings["OPENAI_API_KEY"],
            prompt=settings["AI_PROMPT"],
            df=load_from_csv(settings["PATH_TO_CSV"]),
            column_for_input="Long Description",
            column_for_output="Summary",
            path_to_csv=settings["PATH_TO_CSV"],
        )

        # airtable scripts
        csv_to_airtable(
            airtable_api_token=settings["AIRTABLE_API_TOKEN"],
            airtable_api_url=settings["AIRTABLE_API_URL"],
            csv_filepath=settings["PATH_TO_CSV"],
        )
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
        if view_traceback == "y":
            print(f"\n{traceback_details}\n")


# Run the main function
if __name__ == "__main__":
    main()
