from src.scraper_scripts import scrape_website
from src.csv_scripts import save_to_csv, load_from_csv
from src.airtable_scripts import csv_to_airtable
from src.summary_openai import call_openai
import os

# function parameters
url_setting = "https://www.meetup.com/"

keywords_setting = [
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
]

entry_settings = 100

# openai settings
prompt = "Summarize the following event description in one short and concise sentence. Below the sentence add three bullet points without any emojis. Do not write any headings:\n\n"
text = None  # this is the long description from each event

# csv settings
csv_file_name = "./data/Events_TESTING.csv"

# Get current working directory for filepath
cwd = os.getcwd()
csv_file_path = os.path.join(cwd, csv_file_name)


# main function calling functions
def main():
    # scraper script
    keyword_results_dict = scrape_website(url_setting, keywords_setting, entry_settings)

    # ###
    # OPENAI IF NOT FROM CSV
    # openai api call to summarize
    # openai api call to summarize
    for keyword, event_data_dict in keyword_results_dict.items():
        for event_id, event in event_data_dict.items():
            event["Summary"] = call_openai(prompt, event["Long Description"])

    # ###

    # csv script
    save_to_csv(keyword_results_dict, csv_file_name)
    scraped_data_csv = load_from_csv(csv_file_name)

    # print(scraped_data_csv.head())  # to evaluate the csv

    # # ###
    # # OPENAI IF FROM CSV
    # df = load_from_csv(csv_file_name)

    # # openai api call to summarize
    # for index, row in df.iterrows():
    #     print(f"\nIteration: {index} | {row['Link']}")
    #     print(f"Long Description: {row['Long Description']}")
    #     df.loc[index, "Summary"] = call_openai(prompt, row["Long Description"])
    #     print(f'CSV: {df.loc[index, "Summary"]}\n')

    # df.to_csv(csv_file_path, index=False)
    # # ###

    # airtable script
    csv_to_airtable(csv_file_path)


# Run the main function
if __name__ == "__main__":
    main()
