from src.scraper_scripts import scrape_website
from src.csv_scripts import save_to_csv, load_from_csv
from src.airtable_scripts import csv_to_airtable
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

entry_settings = 60

csv_file_name = "events_scraped_60.csv"

# Get current working directory for filepath
cwd = os.getcwd()
csv_file_path = os.path.join(cwd, csv_file_name)


# main function calling functions
def main():
    # scraper script
    keyword_results_dict = scrape_website(url_setting, keywords_setting, entry_settings)

    # csv script
    save_to_csv(keyword_results_dict, csv_file_name)
    scraped_data_csv = load_from_csv(csv_file_name)

    # print(scraped_data_csv.head())  # to evaluate the csv

    # airtable script
    csv_to_airtable(csv_file_path)


# Run the main function
if __name__ == "__main__":
    main()
