from src.settings_scripts import set_scraper_settings
from src.eventbrite_scripts import get_all_events_info, create_eventbrite_object
from src.meetup_scripts import scrape_meetup
from src.data_scripts import (
    save_dict_to_csv,
    delete_duplicates_add_keywords,
    manipulate_csv_data,
)
from src.openai_scripts import openai_loop_over_column_and_add
from src.airtable_scripts import csv_to_airtable

import pandas as pd
import time

import traceback
import sys


# main function calling functions
def main():
    start_time = time.time()
    print(f"Start: {start_time}")

    # settings["AIRTABLE_API_TOKEN"]
    # settings["AIRTABLE_API_URL"]
    # settings["OPENAI_API_KEY"]
    # settings["EVENTBRITE_PRIVATE_TOKEN"]
    # settings["URL_TO_SCRAPE"]
    # settings["PATH_TO_CSV"]
    # settings["KEYWORDS"]
    # settings["NUMBER_OF_EVENTS_PER_KEYWORD"]
    # settings["AI_PROMPT"]
    # settings["CSV_OPERATIONS"]

    try:
        # # #

        # settings
        # SETTINGS
        settings = set_scraper_settings(
            ai_prompt="Summarize the following event description in one short and concise sentence. Do not include specific information, only focus on the core idea and have an excited tone to it. Do not write any headings. The sentence can be at maximum 125 characters long:",
            keywords=[
                "business",
                "network",
                "conference",
                "entrepreneur",
                "ai",
                "artificial",
                "web3",
                "crypto",
                "blockchain",
                "nft",
                "climate",
                "sustainability",
                "code",
                "coding",
                "tech",
                "robot",
                "design",
                "educat",
                "learn",
                "government",
                "politic",
                "stock",
                "estate",
                "invest",
                "money",
                "finance",
                "tax",
                "career",
                "job",
                "recruit",
                "hiring",
                "hire",
                "journalis",
                "leader",
                "marketing",
                "sales",
                "affiliate",
                "influencer",
                "selling",
                "media",
                "nomad",
                "remote",
                "solopreneur",
                "nonprofit",
                "ngo",
                "charit",
                "product",
                "ui",
                "ux",
                "science",
                "scientific",
                "research",
                "startup",
                "vc",
                "venture",
                "founder",
                "founding",
                "fundrais",
                "funding",
                "accelerator",
                "gründ",
                "vertrieb",
                "führung",
                "finanz",
                "aktie",
                "steuer",
                "immobilie",
                "politik",
                "bildung",
                "klima",
                "nachhaltigkeit",
                "künstliche intelligenz",
                "krypto",
                "beruf",
                "karriere",
                "gehalt",
                "ausbildung",
                "unternehmer",
                "unternehmen",
                "presse",
                "news",
                "forsch",
                "wissenschaft",
                "hackathon",
                "grafik",
            ],
            number_of_events_per_keyword=100,
            csv_operations=[
                {
                    "action": "filter_rows_by_keywords",
                    "columns": ["Name", "Long Description"],
                    "keywords": [  # !!!
                        "founder",
                        "invest",
                        "funding",
                        "entrepreneur",
                        "venture capital",
                        "marketing",
                        "web3",
                        "crypto",
                    ],
                    "skip_columns": ["Keyword"],
                },
                {"action": "add_column", "column_name": "Done", "column_value": "NaN"},
                {
                    "action": "add_column",
                    "column_name": "Calculation",
                    "column_value": "NaN",
                },
                {
                    "action": "add_column",
                    "column_name": "Category",
                    "column_value": "NaN",
                },
                {
                    "action": "add_column",
                    "column_name": "Topics",
                    "column_value": "NaN",
                },
                {
                    "action": "add_column",
                    "column_name": "Chosen (from Topics)",
                    "column_value": "NaN",
                },
                {
                    "action": "add_column",
                    "column_name": "Status (from Topics)",
                    "column_value": "NaN",
                },
                {
                    "action": "add_column",
                    "column_name": "Chosen (from Topics) 2",
                    "column_value": "NaN",
                },
                {
                    "action": "add_column",
                    "column_name": "Topicname (from Topics 2)",
                    "column_value": "NaN",
                },
                {
                    "action": "add_column",
                    "column_name": "Calculation 2",
                    "column_value": "NaN",
                },
                {"action": "uppercase", "column_name": "Keyword"},
            ],
        )
        # meetup collect
        meetup_return = scrape_meetup(
            url=settings["URL_TO_SCRAPE"],
            keywords=settings["KEYWORDS"],
            event_entries=settings["NUMBER_OF_EVENTS_PER_KEYWORD"],
        )
        # eventbrite collect
        eventbrite = create_eventbrite_object(settings["EVENTBRITE_PRIVATE_TOKEN"])
        eventbrite_return = get_all_events_info(
            eventbrite,
            settings["KEYWORDS"],
            "https://www.eventbrite.com/d/",  # Update this URL as required
            "Berlin, Germany",  # Update this location as required
        )
        # saving data to csv
        path_to_csv_test = settings["PATH_TO_CSV"]
        path_to_csv_meetup = (
            path_to_csv_test.rsplit(".", 1)[0]
            + "_MEETUP."
            + path_to_csv_test.rsplit(".", 1)[1]
        )
        path_to_csv_eventbrite = (
            path_to_csv_test.rsplit(".", 1)[0]
            + "_EVENTBRITE."
            + path_to_csv_test.rsplit(".", 1)[1]
        )
        save_dict_to_csv(meetup_return, path_to_csv_meetup)
        save_dict_to_csv(eventbrite_return, path_to_csv_eventbrite)
        # combining data
        df_meetup = pd.read_csv(path_to_csv_meetup)
        df_eventbrite = pd.read_csv(path_to_csv_eventbrite)
        df_combined = pd.concat([df_meetup, df_eventbrite], ignore_index=True)
        # deleting duplicates
        df_combined = delete_duplicates_add_keywords(
            data=df_combined,
            columns_to_compare=["Name", "Long Description", "Date", "Price"],
        )
        # manipulating df
        df_combined = manipulate_csv_data(
            input_df=df_combined,
            operations=settings["CSV_OPERATIONS"],
            output_filepath=settings["PATH_TO_CSV"],
            file_path=None,
        )
        # openai call
        openai_loop_over_column_and_add(
            api_key=settings["OPENAI_API_KEY"],
            prompt=settings["AI_PROMPT"],
            df=df_combined,
            column_for_input="Long Description",
            column_for_output="Summary",
            path_to_csv=settings["PATH_TO_CSV"],
        )
        # airtable call
        csv_to_airtable(
            airtable_api_token=settings["AIRTABLE_API_TOKEN"],
            airtable_api_url=settings["AIRTABLE_API_URL"],
            csv_filepath=settings["PATH_TO_CSV"].rsplit(".", 1)[0]
            + "_SUM."
            + settings["PATH_TO_CSV"].rsplit(".", 1)[1],
        )
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
