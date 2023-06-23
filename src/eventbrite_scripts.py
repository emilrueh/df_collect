from bs4 import BeautifulSoup
import requests
from urllib.parse import quote

from datetime import datetime
from time import sleep

from eventbrite import Eventbrite

import json
import os

from src.data_scripts import get_data_dir

from termcolor import colored


# EVENT IDs
def search_events(search_url, query, location, max_pages=None):
    location = location.replace(", ", "--")
    unique_links = set()

    page = 1
    while True:
        if max_pages and page > max_pages:
            break

        page_url = f"{search_url}{location}/{quote(query)}/?page={page}"
        print(page_url)

        response = requests.get(page_url)
        if response.status_code != 200:
            print(
                colored("SKIP", "yellow"),
                " | ",
                f"Skipping page due to response status code: {colored(response.status_code, 'yellow')}",
            )
            break

        soup = BeautifulSoup(response.content, "html.parser")
        event_links = soup.find_all("a", class_="event-card-link")

        if not event_links:
            break

        unique_links.update(link["href"] for link in event_links)
        page += 1

    return unique_links


def extract_event_id(url):
    if "tickets-" in url:
        event_id = url.split("tickets-")[-1].split("?")[0]
    elif "registrierung-" in url:
        event_id = url.split("registrierung-")[-1].split("?")[0]
    else:
        event_id = None
    return event_id


# EVENT INFO
def create_eventbrite_object(private_token):
    return Eventbrite(oauth_token=private_token)


def get_event_data(eventbrite, event_id):
    try:
        event = eventbrite.get_event(event_id)
        return event
    except Exception as e:
        print(
            colored("SKIP", "yellow"),
            " | ",
            f"Could not get event data for event_id: {event_id} | {colored(e, 'yellow')}",
        )
        return None


def get_venue_data(eventbrite, venue_id):
    try:
        venue = eventbrite.get(f"/venues/{venue_id}/")
        return venue
    except Exception as e:
        print(
            colored("SKIP", "yellow"),
            " | ",
            f"Could not get venue data for venue_id: {venue_id} | {colored(e, 'yellow')}",
        )
        return None


def get_ticket_data(eventbrite, event_id):
    try:
        ticket_classes = eventbrite.get(f"/events/{event_id}/ticket_classes/")
        return ticket_classes
    except Exception as e:
        print(
            colored("SKIP", "yellow"),
            " | ",
            f"Could not get ticket data for event_id: {event_id} | {colored(e, 'yellow')}",
        )
        return None


def get_organizer_data(eventbrite, organizer_id):
    try:
        organizer = eventbrite.get(f"/organizers/{organizer_id}/")
        return organizer
    except Exception as e:
        print(
            colored("SKIP", "yellow"),
            " | ",
            f"Could not get organizer data for organizer_id: {organizer_id} | {colored(e, 'yellow')}",
        )
        return None


# RETURN DICT DATA
def event_info_to_dict(eventbrite, event_id, keyword):
    event_info = get_event_data(eventbrite, event_id)

    if event_info is None:
        print(
            colored("SKIP", "yellow"),
            " | ",
            f"event ID: {event_id} | {colored('missing data', 'yellow')}",
        )
        return None

    date_string = event_info.get("start", {}).get("local")
    if date_string is None:
        print(
            colored("SKIP", "yellow"),
            " | ",
            f"event ID: {event_id} | {colored('missing start or local', 'yellow')}",
        ),

        return None

    date = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S")
    venue_info = get_venue_data(eventbrite, event_info.get("venue_id"))
    ticket_info = get_ticket_data(eventbrite, event_info.get("id"))
    organizer_info = get_organizer_data(eventbrite, event_info.get("organizer_id"))

    return {
        "Name": (event_info.get("name") or {}).get("text"),
        "Photo": (event_info.get("logo") or {}).get("url"),
        "Tags": "",
        "Long_Description": (event_info.get("description") or {}).get("text"),
        "Summary": "",
        "Organizer": (
            organizer_info.get("name") if organizer_info else "Organizer data not found"
        ),
        "Location": "Online event"
        if event_info.get("online_event")
        else (
            lambda v: f'{(v.get("address") or {}).get("address_1")}, {(v.get("address") or {}).get("postal_code")} {(v.get("address") or {}).get("city")}'
        )(venue_info)
        if venue_info is not None
        else "Venue data not found",
        "Venue": "",
        "Gmaps_link": "",
        "Date": date.strftime("%B %d, %Y"),  # 2023-10-11T09:00:00
        "Month": str(date.strftime("%B"))[:3].upper(),
        "Day": date.strftime("%d"),
        "Year": date.strftime("%Y"),
        "Time": date.strftime("%I:%M"),
        "AM_PM": date.strftime("%p"),
        "Price": "Free"
        if event_info.get("is_free")
        else (
            lambda t: f'{((t or [])[0] or {}).get("cost", {}).get("display") if t and t[0] and "cost" in t[0] else "Price not specified"}'
        )((ticket_info or {}).get("ticket_classes")),
        "Link": event_info.get("url"),
        "Keyword": keyword.upper(),
        "Category": "",
        "Source": "Eventbrite",
        "Highlights": "",
        "bookmark_users_id": None,
    }


def get_all_events_info(eventbrite, keywords, search_url, location, max_pages=None):
    all_events_dict = {}
    skipped_events = []

    backup_file_path = get_data_dir() / "runtime_backup_eventbrite.json"
    # Load the backup data if it exists
    if os.path.exists(backup_file_path):
        with open(backup_file_path, "r") as f:
            all_events_dict = json.load(f)

    kw_counter = 0
    total_ev_counter = 0
    sleep(1)
    for keyword in keywords:
        kw_counter += 1
        print(f"{keyword}: {kw_counter}")
        event_urls = search_events(search_url, keyword, location, max_pages)
        event_ids = [extract_event_id(url) for url in event_urls]

        # Check if keyword data is already in the backup
        keyword_events_dict = all_events_dict.get(keyword, {})

        ev_counter = 0

        sleep(2)
        for event_id in event_ids:
            total_ev_counter += 1
            ev_counter += 1
            if event_id is None:
                print(
                    colored("SKIP", "yellow"),
                    " | ",
                    f"{keyword}: {ev_counter} | total: {total_ev_counter} | {colored('event ID is None.', 'yellow')}",
                )
                continue

            # Check if event data is already in the backup
            if keyword_events_dict.get(event_id):
                print(
                    f"{keyword}: {ev_counter} | total: {total_ev_counter} | already scraped."
                )
                continue

            try:
                sleep(1)
                event_info = event_info_to_dict(eventbrite, event_id, keyword)
                print(
                    f"{keyword}: {ev_counter} | total: {total_ev_counter} | {event_info['Link']}"
                )
            except Exception as e:
                print(
                    colored("SKIP", "yellow"),
                    " | ",
                    f"{keyword}: {ev_counter} | total: {total_ev_counter} | {event_info['Link']} | {colored(e, 'yellow')}",
                )
                skipped_events.append(
                    (keyword, event_id)
                )  # keep track of skipped events
                continue

            keyword_events_dict[event_id] = event_info

            # Save data to JSON file after each successful update
            with open(backup_file_path, "w") as f:
                json.dump(all_events_dict, f)

        all_events_dict[keyword] = keyword_events_dict

    # # Rerun the skipped events
    # skip_counter = 0
    # for keyword, event_id in skipped_events:
    #     skip_counter += 1
    #     print(
    #         f"Rerunning skipped event {skip_counter} of {len(skipped_events)} | ID: {event_id} | Keyword: {keyword}"
    #     )
    #     sleep(1)
    #     try:
    #         event_info = event_info_to_dict(eventbrite, event_id, keyword)
    #         if event_info is not None:
    #             sleep(1)
    #             all_events_dict[keyword][event_id] = event_info
    #     except Exception as e:
    #         print(
    #             f"Still cannot process event with ID: {event_id}, skipping again due to exception: {e}"
    #         )

    return all_events_dict
