import json
import requests
import pandas as pd
import numpy as np

import datetime
import requests

import time
from src.token_bucket import TokenBucket

from io import BytesIO
from PIL import Image

import base64

from termcolor import colored


rate_limiter = TokenBucket(10, 0.5)


def api_rate_limit_wait():
    wait_time = rate_limiter.consume(1)  # Consume 1 token for API REQUEST
    if wait_time > 0:
        return wait_time
    else:
        return 0


def try_parsing_date(text):
    for fmt in ("%B %d, %Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.datetime.strptime(text, fmt)
        except ValueError:
            pass
    raise ValueError("no valid date format found")


def convert_image_to_jpg(image_url):
    time.sleep(api_rate_limit_wait())

    response = requests.get(image_url)
    if response.status_code != 200:
        raise Exception(f"Failed to download image from {image_url}")

    img = Image.open(BytesIO(response.content))

    # If the image has a palette, convert it to RGBA
    if img.mode == "P":
        img = img.convert("RGBA")

    with BytesIO() as output:
        img.convert("RGB").save(output, "JPEG")
        output.seek(0)
        return output.read()


def transform_data(event_data):
    transformed = {}
    for k, v in event_data.items():
        if k in ["Date"]:  # timestamp fields
            date = try_parsing_date(v) if v else None
            transformed[k] = date.isoformat() if date else None
        elif k == "Photo" and v:  # handling image
            jpg_image = convert_image_to_jpg(v)
            base64_image = base64.b64encode(jpg_image).decode("utf-8")
            transformed[k] = f"data:image/jpeg;base64,{base64_image}"
        else:  # all other fields
            transformed[k] = v
    return transformed


def check_existing_records(headers, get_all_endpoint_url):
    time.sleep(api_rate_limit_wait())

    response = requests.get(get_all_endpoint_url, headers=headers)
    if response.status_code != 200:
        print(colored("Failed to retrieve data from the database.", "yellow"))
        raise Exception(
            f"Request failed with status {response.status_code}. Message: {response.text}"
        )
    existing_entries = response.json()

    return {
        entry["Link"]: {
            "id": entry["id"],
            "entry": entry,
            "Highlights": entry.get("Highlights"),
            "bookmark_users_id": entry.get("bookmark_users_id"),
        }
        for entry in existing_entries
    }


def entries_are_equal(entry1, entry2):
    keys_to_compare = [
        "bookmark_users_id",
        "AM_PM",
        "Link",
        "Name",
        "Organizer",
        "Venue",
        "Month",
        "Year",
        "Keyword",
        "Photo",
        "Highlights",
        "Time",
        "Summary",
        "Gmaps_link",
        "Location",
        "Price",
        "Date",
        "Long_Description",
        "Day",
        "Category",
        "Source",
        "Tags",
        "Archived",
    ]

    for key in keys_to_compare:
        if key == "Photo":
            # handle the Photo field here
            continue
        value1 = entry1.get(key)
        value2 = entry2.get(key)
        if key == "Date" and value1 and value2:
            # parse and compare only the date parts (year, month, day)
            date1 = datetime.datetime.fromisoformat(value1)
            date2 = datetime.datetime.fromisoformat(
                value2 if "T" in value2 else value2 + "T00:00:00"
            )
            if (date1.year, date1.month, date1.day) != (
                date2.year,
                date2.month,
                date2.day,
            ):
                return False
        elif key == "Highlights" and (
            value1 == 0.0 and value2 is None or value1 is None and value2 == 0.0
        ):
            # Treat 0.0 and None as equal for the Highlights key
            continue
        elif (
            isinstance(value1, float) and isinstance(value2, list) and len(value2) == 1
        ):
            if value1 != float(value2[0]):
                return False
        elif value1 != value2:
            return False
    return True


def send_request_with_retry(url, method, headers, data=None, max_attempts=3):
    time.sleep(api_rate_limit_wait())

    for attempt in range(max_attempts):
        try:
            if method == "POST":
                response = requests.post(url, headers=headers, data=data)
            elif method == "GET":
                response = requests.get(url, headers=headers)
            elif method == "DELETE":
                response = requests.delete(url, headers=headers, data=data)

            response.raise_for_status()
        except (
            requests.exceptions.RequestException,
            requests.exceptions.ConnectTimeout,
            requests.exceptions.HTTPError,
            Exception,
        ) as e:
            wait_time = 2**attempt
            print(
                f"\n{colored(f'Attempt {attempt+1}', 'light_red')} failed with error: {e}.\nRetrying in {wait_time} seconds...",
            )
            time.sleep(wait_time)
            if attempt == max_attempts - 1:
                print(colored("\nALL FAILED!", color="light_red", attrs=["bold"]))
                try:
                    print(f"Response body: {response.text}")
                except UnboundLocalError:
                    print("Response object is not available.")
                raise
        else:
            return response
    return None  # This should never be reached but is included for completeness


def update_entry_and_print(
    transformed_row, headers, edit_url, current_event_id, row_index
):
    response = send_request_with_retry(
        edit_url, "POST", headers, json.dumps(transformed_row)
    )
    print(
        row_index,
        colored("UPDATED", color="light_yellow", attrs=["bold"]),
        f"ID {colored(current_event_id, color='magenta')} {transformed_row['Link']}",
    )
    response_json = response.json()
    return response_json


def skip_entry_and_print(row_index, current_event_id, link):
    if current_event_id == None:
        print(
            row_index,
            colored("SKIPPED", color="light_green", attrs=["bold"]),
            f"as past {link}",
        )
    else:
        print(
            row_index,
            colored("SKIPPED", color="light_green", attrs=["bold"]),
            f"ID {colored(current_event_id, 'magenta')} {link}",
        )


def check_existing_records(headers, get_all_endpoint_url):
    time.sleep(api_rate_limit_wait())

    response = requests.get(get_all_endpoint_url, headers=headers)
    if response.status_code != 200:
        print(colored("Failed to retrieve data from the database.", "yellow"))
        raise Exception(
            f"Request failed with status {response.status_code}. Message: {response.text}"
        )
    existing_entries = response.json()

    return {
        entry["Link"]: {
            # "id": entry["id"],
            "entry": entry,
            "Highlights": entry.get("Highlights"),
            "bookmark_users_id": entry.get("bookmark_users_id"),
        }
        for entry in existing_entries
    }


def send_request_with_retry(url, method, headers, data=None, max_attempts=3):
    time.sleep(api_rate_limit_wait())

    for attempt in range(max_attempts):
        try:
            if method == "POST":
                response = requests.post(url, headers=headers, data=data)
            elif method == "GET":
                response = requests.get(url, headers=headers)
            elif method == "DELETE":
                response = requests.delete(url, headers=headers, data=data)

            response.raise_for_status()
        except (
            requests.exceptions.RequestException,
            requests.exceptions.ConnectTimeout,
            requests.exceptions.HTTPError,
            Exception,
        ) as e:
            wait_time = 2**attempt
            print(
                f"\n{colored(f'Attempt {attempt+1}', 'light_red')} failed with error: {e}.\nRetrying in {wait_time} seconds...",
            )
            time.sleep(wait_time)
            if attempt == max_attempts - 1:
                print(colored("\nALL FAILED!", color="light_red", attrs=["bold"]))
                try:
                    print(f"Response body: {response.text}")
                except UnboundLocalError:
                    print("Response object is not available.")
                raise
        else:
            return response
    return None  # This should never be reached but is included for completeness


def update_entry_and_print(
    transformed_row, headers, edit_url, current_event_id, row_index
):
    response = send_request_with_retry(
        edit_url, "POST", headers, json.dumps(transformed_row)
    )
    if current_event_id == row_index:
        print(
            "    ",
            colored("ARCHIVE", color="white", attrs=["bold"]),
            f"ID {colored(current_event_id, color='magenta')} {transformed_row['Link']}",
        )
    else:
        print(
            row_index,
            colored("UPDATED", color="light_yellow", attrs=["bold"]),
            f"ID {colored(current_event_id, color='magenta')} {transformed_row['Link']}",
        )
    response_json = response.json()
    return response_json


def skip_entry_and_print(row_index, current_event_id, link):
    if current_event_id == None:
        print(
            row_index,
            colored("SKIPPED", color="light_green", attrs=["bold"]),
            f"as past {link}",
        )
    else:
        print(
            row_index,
            colored("SKIPPED", color="light_green", attrs=["bold"]),
            f"ID {colored(current_event_id, 'magenta')} {link}",
        )


def send_data_to_xano(
    data,
    api_key,
    image_endpoint_url,
    send_endpoint_url,
    check_endpoint_url,
    edit_endpoint_url,
    delete_endpoint_url,
    table_name,
):
    if isinstance(data, pd.DataFrame):
        data = data.replace({np.nan: None})
        data = data.to_dict(orient="records")

    headers = {
        "Authorization": f"Bearer {api_key}",
    }

    response_list = []

    print(colored(f"Accessing Xano data base {table_name}...\n", "cyan"))
    table_name = table_name.replace(" ", "_").lower()

    existing_entries = check_existing_records(
        headers=headers, get_all_endpoint_url=check_endpoint_url
    )

    today = datetime.datetime.now().date()  # get the date part of the current datetime

    #
    # loop over existing entries for archive/delete
    for link, entry in existing_entries.items():
        event_date_str = entry["entry"]["Date"].split("T")[0]
        event_date = datetime.datetime.strptime(event_date_str, "%Y-%m-%d").date()

        # Updating archived or deleting old events
        if event_date < today:
            current_event_id = entry["entry"]["id"]
            if (  # checks if user has bookmarked this past event
                entry["entry"]["bookmark_users_id"] != [0]
                and entry["entry"]["bookmark_users_id"] != []
                and entry["entry"]["bookmark_users_id"] != ()
                and entry["entry"]["bookmark_users_id"] != 0
                and entry["entry"]["bookmark_users_id"] != None
            ):  # checks if user has bookmarked this past event
                if entry["entry"]["Archived"]:  # checks if event is already archived
                    continue

                entry["entry"]["Archived"] = True  # sets archived field to true

                edit_url = edit_endpoint_url.format(id=current_event_id)

                entry["entry"][f"{table_name}_id"] = entry["entry"].pop(
                    "id"
                )  # changing name of id key

                response_json = update_entry_and_print(
                    entry["entry"],
                    headers,
                    edit_url,
                    current_event_id,
                    current_event_id,
                )
                response_list.append(response_json)

            else:
                # Delete the entry
                if current_event_id:
                    delete_url = delete_endpoint_url.format(id=current_event_id)
                    delete_param = {f"{table_name}_id": int(current_event_id)}
                    response = send_request_with_retry(
                        delete_url,
                        "DELETE",
                        headers,
                        data=json.dumps(delete_param),
                    )
                    print(
                        "    ",
                        colored("DELETED", color="light_magenta", attrs=["bold"]),
                        f"ID {colored(current_event_id, 'magenta')} {link}",
                    )
                    # del existing_entries[link]  # delete the entry from existing_entries

    # loop over new data from df
    for i, row in enumerate(data):
        row_index = i + 1
        row_index = f"{row_index:4}"

        transformed_row = transform_data(row)
        link = transformed_row["Link"]

        # Process image metadata
        if "Photo" in transformed_row and transformed_row["Photo"]:
            base64_image = transformed_row["Photo"]
            response = send_request_with_retry(
                image_endpoint_url,
                "POST",
                headers,
                json.dumps({"content": base64_image}),
            )

            image_metadata = response.json()
            transformed_row["Photo"] = [
                {
                    "path": image_metadata["path"],
                    "name": image_metadata["name"],
                    "type": image_metadata["type"],
                    "size": image_metadata["size"],
                    "mime": image_metadata["mime"],
                    "meta": image_metadata["meta"],
                }
            ]

        # check if the current event is in the existing entries and if so, set current_event_id
        # if link in existing_entries:
        #     print(existing_entries[link]["entry"]["id"])
        current_event_id = (
            existing_entries[link]["entry"]["id"] if link in existing_entries else None
        )
        colored_event_id = colored(current_event_id, "magenta")

        # Skip events with a date in the past
        if "Date" in transformed_row:
            event_date_str = transformed_row["Date"].split("T")[0]
            event_date = datetime.datetime.strptime(
                event_date_str, "%Y-%m-%d"
            ).date()  # convert the date string to a date object

            if event_date < today:
                skip_entry_and_print(
                    row_index, current_event_id, transformed_row["Link"]
                )
                continue  # move to the next row in data if this event is in the past

        if current_event_id:
            if entries_are_equal(
                {
                    k: v
                    for k, v in transformed_row.items()
                    if k not in ["bookmark_users_id", "Highlights"]
                },
                {
                    k: v
                    for k, v in existing_entries[link]["entry"].items()
                    if k not in ["bookmark_users_id", "Highlights"]
                },
            ):
                skip_entry_and_print(
                    row_index, current_event_id, transformed_row["Link"]
                )
                continue
            else:
                edit_url = edit_endpoint_url.format(id=current_event_id)

                if existing_entries[link]["entry"]["bookmark_users_id"]:
                    transformed_row["bookmark_users_id"] = existing_entries[link][
                        "entry"
                    ]["bookmark_users_id"]

                if existing_entries[link]["entry"]["Highlights"]:
                    transformed_row["Highlights"] = existing_entries[link]["entry"][
                        "Highlights"
                    ]

                transformed_row[f"{table_name}_id"] = current_event_id

                response_json = update_entry_and_print(
                    transformed_row, headers, edit_url, current_event_id, row_index
                )

                response_list.append(response_json)
                continue

        row_json = json.dumps(transformed_row)
        headers.update({"Content-Type": "application/json"})

        response = send_request_with_retry(send_endpoint_url, "POST", headers, row_json)

        if response.status_code == 200:
            current_event_id = response.json()["id"]
            print(
                row_index,
                colored("CREATED", color="light_blue", attrs=["bold"]),
                f"ID {colored(current_event_id, 'magenta')} {transformed_row['Link']}",
            )

        response_json = response.json()
        response_list.append(response_json)

    return response_list
