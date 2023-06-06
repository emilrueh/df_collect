# selenium imports
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from datetime import datetime
import time
import os
import json

from src.data_scripts import json_save


# main loop function
def scrape_meetup(url, keywords, event_entries):
    keyword_results_dict = {}
    if os.path.exists("meetup_runtime_backup.json"):
        with open("meetup_runtime_backup.json", "r") as f:
            keyword_results_dict = json.load(f)

    browser = webdriver.Firefox()

    processed_events_urls = set()

    total_event_counter = 0
    # main loop
    # find search bar and enter the keywords
    for word in keywords:
        # print(word)  # to see the progress of the script
        event_data_dict = keyword_results_dict.get(word, {})

        browser.get(url)  # Start from the fresh page for each keyword
        time.sleep(5)  # Let the page load

        # COOKIES
        try:
            cookie_settings = browser.find_element(
                By.XPATH, '//*[@id="onetrust-pc-btn-handler"]'
            )
            cookie_settings.click()

            cookie_deny = browser.find_element(
                By.XPATH, "/html/body/div[4]/div[3]/div[3]/div[1]/button[1]"
            )
            cookie_deny.click()

        except NoSuchElementException:
            pass
        time.sleep(5)

        # KEYWORD SEARCHING
        try:
            search_bar = browser.find_element(
                By.XPATH, '//*[@id="keyword-bar-in-search"]'
            )

        except NoSuchElementException:
            search_bar = browser.find_element(
                By.XPATH, '//*[@id="search-keyword-input"]'
            )
        time.sleep(1)

        search_bar.send_keys(word)

        time.sleep(2)

        try:
            location_bar = browser.find_element(
                By.XPATH, '//*[@id="location-typeahead-header-search"]'
            )

        except NoSuchElementException:
            try:
                location_bar = browser.find_element(
                    By.XPATH, '//*[@id="location-typeahead-searchLocation"]'
                )

            except NoSuchElementException:
                location_bar = browser.find_element(
                    By.XPATH, '//*[@id="location-bar-in-search"]'
                )
        location_bar.send_keys("Berlin")

        time.sleep(1)
        # Send the Arrow Down key
        location_bar.send_keys(Keys.ARROW_DOWN)
        time.sleep(1)
        # Send the Enter key
        location_bar.send_keys(Keys.ENTER)
        time.sleep(4)

        # close hint popup
        try:
            popup = browser.find_element(
                By.XPATH,
                "/html/body/div[1]/div[2]/div[2]/header/div[2]/div/div[1]/div[2]/div/form/div/div[2]/div/div/div[2]/button/img",
            )
            popup.click()

        except NoSuchElementException:
            pass

        try:
            search_button = browser.find_element(
                By.XPATH,
                '//*[@id="search-button"]',
            )

        except NoSuchElementException:
            search_button = browser.find_element(
                By.XPATH,
                "/html/body/div[1]/div[2]/div[2]/div/div[2]/main/div[1]/div[4]/div/form/div[2]/input",
            )
        search_button.click()

        time.sleep(4)

        # EVENT URLS
        event_url_list = []

        # endless scrolling
        while len(event_url_list) < event_entries:
            events = browser.find_elements(
                By.XPATH, '//*[@id="event-card-in-search-results"]'
            )

            for event in events:
                event_info = event.text
                event_url = event.get_attribute("href")
                event_url_list.append(event_url)

                if len(event_url_list) == event_entries:
                    break  # Reached the maximum number of entries, exit the loop

            # Scroll to trigger loading more events
            browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)  # Adjust the sleep time as needed to allow for event loading

            # Check if no more new events are loaded
            if len(events) == 0:
                break

        # EVENT INFO
        # Initialize an empty dictionary
        event_data_dict = {}

        # loop through event links and copy info
        event_per_keyword_counter = 0
        for event in event_url_list:
            # If this event's URL is already in processed_events_urls, skip it
            event_id = event.split("/")[-1]
            if event in processed_events_urls or event_id in event_data_dict:
                continue

            # progress tracker
            event_per_keyword_counter += 1
            total_event_counter += 1
            print(
                f"{word}: {event_per_keyword_counter} | Total: {total_event_counter} | {event}"
            )

            browser.get(event)  # open event link
            time.sleep(5)

            #
            # title
            try:
                event_title = browser.find_element(
                    By.XPATH,
                    "/html/body/div[1]/div[2]/div[2]/div[2]/main/div[2]/div/h1",
                ).text

            except NoSuchElementException:
                event_title = "NaN"

            # description
            try:
                event_description = browser.find_element(
                    By.XPATH,
                    '//*[@id="event-details"]',
                ).text

            except NoSuchElementException:
                event_description = "NaN"

            # ORGANIZER
            try:
                event_organizer = browser.find_element(
                    By.XPATH, '//*[@id="event-group-link"]/div/div[2]/div[1]'
                ).text
            except NoSuchElementException:
                event_organizer = "NaN"

            # date
            try:
                event_date = browser.find_element(
                    By.XPATH,
                    "/html/body/div[1]/div[2]/div[2]/div[2]/main/div[3]/div[1]/div/div[2]/div[2]/div[2]/div[1]/div[1]/div[1]/div/div[2]/div/time",
                ).text

            except NoSuchElementException:
                try:
                    event_date = browser.find_element(
                        By.XPATH,
                        "/html/body/div[1]/div[2]/div[2]/div[2]/main/div[3]/div[1]/div/div[2]/div[2]/div[2]/div/div[1]/div[2]/div[1]/div[2]/div/time",
                    ).text

                except NoSuchElementException:
                    event_date = "NaN"

            # date and time splitting
            if event_date != "NaN":
                date_time_parts = event_date.split(" at ")
                date_parts = date_time_parts[0].split(", ")
                time_parts = date_time_parts[1].split(" to ")

                # The second part of date_parts will be 'June 9, 2023' which is your event_start_date
                event_start_date = ", ".join(date_parts[1:])

                # The first part of time_parts will be '7:00 PM' which is your event_start_time
                # Let's split this into the time and the am/pm part
                event_start_time_parts = time_parts[0].split(" ")
                event_start_time = event_start_time_parts[
                    0
                ]  # This is the time without the am/pm part
                event_start_am_pm = event_start_time_parts[1]  # This is the am/pm part

            else:
                event_start_date = "NaN"
                event_start_time = "NaN"
                event_start_am_pm = "NaN"

            # location
            try:
                event_location_element = browser.find_element(
                    By.XPATH,
                    "/html/body/div[1]/div[2]/div[2]/div[2]/main/div[3]/div[1]/div/div[2]/div[2]/div[2]/div[1]/div[1]/div[3]/div[2]/a",
                )
                event_location = event_location_element.text
                event_maps_link = event_location_element.get_attribute("href")
                event_address = browser.find_element(
                    By.XPATH,
                    "/html/body/div[1]/div[2]/div[2]/div[2]/main/div[3]/div[1]/div/div[2]/div[2]/div[2]/div[1]/div[1]/div[3]/div[2]/div",
                ).text

            except NoSuchElementException:
                try:
                    event_location = browser.find_element(
                        By.XPATH,
                        "/html/body/div[1]/div[2]/div[2]/div[2]/main/div[3]/div[1]/div/div[2]/div[2]/div[2]/div/div[1]/div[3]/div[2]/div[1]",
                    ).text
                    event_address = event_location
                    event_maps_link = "NaN"

                except NoSuchElementException:
                    event_location = "NaN"
                    event_address = "NaN"
                    event_maps_link = "NaN"

            # tags
            try:
                event_tags_element = browser.find_element(By.XPATH, '//*[@id="topics"]')
                event_tags = event_tags_element.text.replace("\n", ",")

            except NoSuchElementException:
                event_tags = "NaN"

            # image
            try:
                event_image_element = browser.find_element(
                    By.XPATH,
                    "/html/body/div[1]/div[2]/div[2]/div[2]/main/div[3]/div[1]/div/div[1]/div/div[1]/div[1]/picture/div/img",
                )
                event_image = event_image_element.get_attribute("src")

            except NoSuchElementException:
                event_image = "NaN"

            # price
            try:
                event_price = browser.find_element(
                    By.XPATH,
                    '//*[@id="main"]/div[4]/div/div/div[2]/div/div[1]/div/div/span',
                ).text

            except NoSuchElementException:
                event_price = "NaN"

            #
            # dictionary
            event_data = {
                "Name": event_title,
                "Photo": event_image,
                "Tags": event_tags,
                "Long Description": (
                    event_description.replace("Details\n", "")
                    if "Details\n" in event_description
                    else event_description
                ),
                "Summary": "",
                "Organizer": event_organizer,
                "Location": event_address,
                "Venue": event_location,
                "Gmaps link": event_maps_link,
                "Date": event_start_date,  # June 9, 2023 7:00
                "Month": datetime.strptime(event_start_date, "%B %d, %Y")
                .strftime("%B")[:3]
                .upper()
                if event_start_date != "NaN"
                else "NaN",
                "Day": datetime.strptime(event_start_date, "%B %d, %Y").strftime("%d")
                if event_start_date != "NaN"
                else "NaN",
                "Year": datetime.strptime(event_start_date, "%B %d, %Y").strftime("%Y")
                if event_start_date != "NaN"
                else "NaN",
                "Time": event_start_time if event_start_time != "NaN" else "NaN",
                "AM/PM": event_start_am_pm if event_start_am_pm != "NaN" else "NaN",
                "Price": event_price.title(),
                "Link": event,
                "Keyword": word,
                "Source": "Meetup",
                "Category": "",
                "People": "",
                "Group Size": "",
            }

            # Add this event's URL to processed_events_urls
            processed_events_urls.add(event)

            # dict of dict
            # Store the event_data dictionary in event_data_dict, using the event title as the key
            # event_id = event.split("/")[-1]
            event_data_dict[event_id] = event_data

            with open("meetup_runtime_backup.json", "w") as f:
                json.dump(keyword_results_dict, f)

        # dict of dict of dict
        # Store the results for this keyword
        keyword_results_dict[word] = event_data_dict

    browser.quit()

    json_save(keyword_results_dict, "meetup_runtime_backup.json")

    return keyword_results_dict
