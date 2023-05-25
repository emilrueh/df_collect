# Event Scraper

This project is a Python script that uses Selenium to scrape event data from a website. 

## Prerequisites

To run this script, you'll need:

- Python 3.6 or later
- Selenium
- Firefox
- geckodriver.log

Make sure you have Firefox installed and download the `geckodriver.log` executable from the [official website](https://github.com/mozilla/geckodriver/releases). Make sure the `geckodriver.log` is available in your system's PATH or place it in the same directory as your script.

## Usage

1. Update the `url`, `keywords`, and `event_entries` parameters in the `scrape_website` function as needed.
2. Check the other function calls for saving and loading the .csv and adjust the file name.
3. Run the Python script.

```bash
python main.py


This script will open a new Firefox window controlled by Selenium, navigate to the specified url, and search for events based on the keywords. It will continue to scroll and collect data until it reaches the number of event_entries specified.

The script will collect the following data for each event:

Name
Photo
Category
Tags
People
Group Size
Long Description
Summary
Location
Venue
Gmaps link
Date
Time
Price
Link
Keyword

The scraped data is returned as a nested dictionary, where the outer dictionary keys are the keywords and the inner dictionary keys are the event URLs.

After scraping the data, the script will upload the data to Airtable using the csv_to_airtable function. You must provide your own Airtable API key and URL for this to work.