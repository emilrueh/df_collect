import logging
from src.helper_utils import main_logger_name

logger_name = main_logger_name
logger = logging.getLogger(logger_name)


import pandas as pd
import numpy as np
import json

import requests

import time
import datetime
from datetime import timedelta

from termcolor import colored

from io import BytesIO
from PIL import Image
import base64

from typing import Union, Optional, Dict, Any

from src.token_bucket import TokenBucket


HTTP_POST = "POST"
HTTP_GET = "GET"
HTTP_DELETE = "DELETE"


rate_limiter = TokenBucket(tokens=10, fill_rate=0.2)


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
            logger.debug(f"Failed to parse date with format {fmt}")
            pass
    logger.error("No valid date format found for text: {text}")
    raise ValueError("No valid date format found")


def convert_image_to_jpg(image_url):
    # Check if the URL is valid
    if pd.isna(image_url) or image_url == "NaN":
        logger.warning(f"Invalid URL: {image_url}")
        return None

    time.sleep(api_rate_limit_wait())

    response = requests.get(image_url)
    if response.status_code != 200:
        logger.error(f"Failed to download image from {image_url}")
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
            if jpg_image is not None:
                base64_image = base64.b64encode(jpg_image).decode("utf-8")
                transformed[k] = f"data:image/jpeg;base64,{base64_image}"
            else:
                transformed[k] = None
        else:  # all other fields
            transformed[k] = v
    return transformed


def check_existing_records(headers, get_all_endpoint_url):
    time.sleep(api_rate_limit_wait())

    response = requests.get(get_all_endpoint_url, headers=headers)
    if response.status_code != 200:
        error_message = (
            f"Request failed with status {response.status_code}. "
            f"Message: {response.text}"
        )
        logger.error("Failed to retrieve data from the database.")
        logger.error(error_message)
        logger.debug("Exception details: ", exc_info=True)
        raise Exception(error_message)
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


def send_request_with_retry(
    url: str,
    method: str,
    headers: Dict[str, str],
    data: Optional[Union[str, bytes]] = None,
    max_attempts: int = 6,
) -> requests.Response:
    #
    DEFAULT_RETRY_AFTER = 6
    failure_occurred = False

    for attempt in range(max_attempts):
        time.sleep(api_rate_limit_wait())
        try:
            if method == HTTP_POST:
                response = requests.post(url, headers=headers, data=data)
            elif method == HTTP_GET:
                response = requests.get(url, headers=headers)
            elif method == HTTP_DELETE:
                response = requests.delete(url, headers=headers, data=data)

            # checking Retry-After header and sleeping if necessary
            if response.status_code == 429:
                sleep_duration = response.headers.get(
                    "Retry-After", DEFAULT_RETRY_AFTER
                )
                time.sleep(int(sleep_duration))

            response.raise_for_status()
        except (
            requests.exceptions.RequestException,
            requests.exceptions.ConnectTimeout,
            requests.exceptions.HTTPError,
            Exception,
        ) as e:
            failure_occurred = True
            wait_time = 2 ** (attempt + 1)
            logger.warning(
                f"{colored(f'Attempt {attempt+1}', 'light_red')} failed with error: {e}.\nRetrying in {wait_time} seconds...",
            )
            # Custom warning message
            if attempt > 0 and isinstance(
                e, (requests.exceptions.HTTPError, requests.exceptions.ConnectTimeout)
            ):
                total_wait_time_minutes = (
                    sum(2**i for i in range(2, max_attempts + 2)) / 60
                )
                logger.warning(
                    f"Please note that probably due to server-side issues the script might take more than {total_wait_time_minutes:.2f} minutes longer than usual to complete."
                )
            time.sleep(wait_time)
            if attempt == max_attempts - 1:
                logger.critical(
                    colored("ALL FAILED!", color="light_red", attrs=["bold"])
                )
                try:
                    logger.critical(f"Response body: {response.text}")
                except UnboundLocalError:
                    logger.critical("Response object is not available.")
                logger.exception("Exception details: ")
                raise
        else:
            if failure_occurred:
                logger.warning(
                    f"{colored(f'Attempt {attempt+1}', 'light_green')} was successful after {attempt} failed attempt{'s 'if attempt > 1 else ''}."
                )
            # Reset the failure flag for next function call
            failure_occurred = False
            return response
    return None  # This should never be reached but is included for completeness


# def send_request_with_retry(
#     url: str,
#     method: str,
#     headers: Dict[str, str],
#     data: Optional[Union[str, bytes]] = None,
#     max_attempts: int = 6,
# ) -> requests.Response:
#     DEFAULT_RETRY_AFTER = 3
#     for attempt in range(max_attempts):
#         time.sleep(api_rate_limit_wait())
#         try:
#             if method == HTTP_POST:
#                 response = requests.post(url, headers=headers, data=data)
#             elif method == HTTP_GET:
#                 response = requests.get(url, headers=headers)
#             elif method == HTTP_DELETE:
#                 response = requests.delete(url, headers=headers, data=data)

#             # checking Retry-After header and sleeping if necessary
#             if response.status_code == 429:
#                 sleep_duration = response.headers.get(
#                     "Retry-After", DEFAULT_RETRY_AFTER
#                 )
#                 time.sleep(int(sleep_duration))

#             response.raise_for_status()
#         except (
#             requests.exceptions.RequestException,
#             requests.exceptions.ConnectTimeout,
#             requests.exceptions.HTTPError,
#             Exception,
#         ) as e:
#             wait_time = 2 ** (attempt + 1)
#             logger.warning(
#                 f"\n{colored(f'Attempt {attempt+1}', 'light_red')} failed with error: {e}.\nRetrying in {wait_time} seconds...",
#             )
#             # Custom warning message
#             if attempt > 0 and isinstance(
#                 e, (requests.exceptions.HTTPError, requests.exceptions.ConnectTimeout)
#             ):
#                 total_wait_time_minutes = (
#                     sum(2**i for i in range(2, max_attempts + 2)) / 60
#                 )
#                 logger.warning(
#                     f"Please note that probably due to server-side issues the script might take more than {total_wait_time_minutes:.2f} minutes longer than usual to complete."
#                 )
#             time.sleep(wait_time)
#             if attempt == max_attempts - 1:
#                 logger.critical(
#                     colored("\nALL FAILED!", color="light_red", attrs=["bold"])
#                 )
#                 try:
#                     logger.critical(f"Response body: {response.text}")
#                 except UnboundLocalError:
#                     logger.critical("Response object is not available.")
#                 logger.exception("Exception details: ")
#                 raise
#         else:
#             return response
#     return None  # This should never be reached but is included for completeness


def update_entry_and_print(
    transformed_row, headers, edit_url, current_event_id, row_index
):
    response = send_request_with_retry(
        edit_url, "POST", headers, json.dumps(transformed_row)
    )
    if current_event_id == row_index:
        logger.info(
            f"    {colored('ARCHIVE', color='white', attrs=['bold'])} ID {colored(current_event_id, color='magenta')} {transformed_row['Link']}"
        )
    else:
        logger.info(
            f"{row_index} {colored('UPDATED', color='light_yellow', attrs=['bold'])} ID {colored(current_event_id, color='magenta')} {transformed_row['Link']}"
        )
    response_json = response.json()
    return response_json


def skip_entry_and_print(row_index, current_event_id, link):
    if current_event_id == None:
        logger.info(
            f"{row_index} {colored('SKIPPED', color='light_green', attrs=['bold'])} as past {link}"
        )
    else:
        logger.info(
            f"{row_index} {colored('SKIPPED', color='light_green', attrs=['bold'])} ID {colored(current_event_id, 'magenta')} {link}"
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
    update_summary: bool = True,
):
    if isinstance(data, pd.DataFrame):
        data = data.replace({np.nan: None})
        data = data.to_dict(orient="records")

    headers = {
        "Authorization": f"Bearer {api_key}",
    }

    response_list = []

    logger.info(colored(f"Accessing Xano data base {table_name}...\n", "cyan"))
    table_name = table_name.replace(" ", "_").lower()

    existing_entries = check_existing_records(
        headers=headers, get_all_endpoint_url=check_endpoint_url
    )

    yesterday = datetime.datetime.now().date() - timedelta(days=1)

    #
    # loop over existing entries for archive/delete
    for link, entry in existing_entries.items():
        if entry["entry"]["Date"]:
            event_date_str = entry["entry"]["Date"].split("T")[0]
            event_date = datetime.datetime.strptime(event_date_str, "%Y-%m-%d").date()

        # Updating archived or deleting old events
        if event_date < yesterday:
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
                    time.sleep(api_rate_limit_wait())
                    response = send_request_with_retry(
                        delete_url,
                        "DELETE",
                        headers,
                        data=json.dumps(delete_param),
                    )
                    logger.info(
                        f"    {colored('DELETED', color='light_magenta', attrs=['bold'])} ID {colored(current_event_id, 'magenta')} {link}"
                    )
                    # del existing_entries[link]  # delete the entry from existing_entries

    # Create list of fields to ignore dynamically
    ignored_fields = ["bookmark_users_id", "Highlights"]
    if not update_summary:
        ignored_fields.append("Summary")

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
            if transformed_row["Date"]:
                event_date_str = transformed_row["Date"].split("T")[0]
                event_date = datetime.datetime.strptime(
                    event_date_str, "%Y-%m-%d"
                ).date()  # convert the date string to a date object

            if event_date < yesterday:
                skip_entry_and_print(
                    row_index, current_event_id, transformed_row["Link"]
                )
                continue  # move to the next row in data if this event is in the past

        if current_event_id:
            if entries_are_equal(
                {k: v for k, v in transformed_row.items() if k not in ignored_fields},
                {
                    k: v
                    for k, v in existing_entries[link]["entry"].items()
                    if k not in ignored_fields
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
            logger.info(
                f"{row_index} {colored('CREATED', color='light_blue', attrs=['bold'])} ID {colored(current_event_id, 'magenta')} {transformed_row['Link']}"
            )

        response_json = response.json()
        response_list.append(response_json)

    return response_list
