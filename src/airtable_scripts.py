# airtable imports
import requests
import json
import csv
import os
from time import sleep
from dotenv import load_dotenv


# loading dotenv
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".env"))

attempts = 3

while attempts > 0:
    load_dotenv(dotenv_path)

    # Get environment variables
    AIRTABLE_API_BASE_ID = os.getenv("AIRTABLE_API_BASE_ID")
    AIRTABLE_API_TABLE_NAME = os.getenv("AIRTABLE_API_TABLE_NAME")
    AIRTABLE_API_TOKEN = os.getenv("AIRTABLE_API_TOKEN")

    AIRTABLE_API_URL = (
        f"https://api.airtable.com/v0/{AIRTABLE_API_BASE_ID}/{AIRTABLE_API_TABLE_NAME}"
    )

    # check if no variables loaded
    if (
        not AIRTABLE_API_BASE_ID
        or not AIRTABLE_API_TABLE_NAME
        or not AIRTABLE_API_TOKEN
    ):
        print("One or more environment variables not found. Retrying...")
        attempts -= 1
        sleep(3)
    else:
        break

# print error after three attempts
if attempts == 0:
    raise ValueError(
        "Failed to load one or more environment variables after 3 attempts."
    )


# airtable functions
def transform_data(row):
    transformed_row = {
        "Name": row["Name"],
        "Photo": [{"url": row["Photo"]}] if row["Photo"] else [],
        "Category": row["Category"].split(", ") if row["Category"] else [],
        "Tags": row["Tags"],
        "People": row["People"].split(", ") if row["People"] else [],
        "Group Size": row["Group Size"] if row["Group Size"] else None,
        "Long Description": row["Long Description"],
        "Summary": row["Summary"],
        "Location": row["Location"],
        "Venue": row["Venue"],
        "Gmaps link": row["Gmaps link"],
        "Date": row["Date"] if row["Date"] != "NaN" else None,
        "Time": row["Time"] if row["Time"] != "NaN" else None,
        "Price": row["Price"],
        "Link": row["Link"],
        "Keyword": row["Keyword"],
    }
    return transformed_row


def fetch_existing_records():
    headers = {
        "Authorization": "Bearer " + AIRTABLE_API_TOKEN,
        "Content-Type": "application/json",
    }

    existing_records = set()
    offset = None
    try:
        while True:
            params = (
                {
                    "offset": offset,
                }
                if offset
                else {}
            )
            response = requests.get(AIRTABLE_API_URL, headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()
                records = data.get("records", [])
                if not records:  # No existing records
                    break
                existing_records.update(record["fields"]["Name"] for record in records)
                if "offset" in data:
                    offset = data["offset"]
                else:
                    break
            else:
                print(f"Error fetching existing records: {response.text}")
                return set()  # Return an empty set
    except Exception as e:
        print(f"Error fetching existing records: {str(e)}")
        return set()  # Return an empty set

    return existing_records


def csv_to_airtable(csv_filepath):
    headers = {
        "Authorization": "Bearer " + AIRTABLE_API_TOKEN,
        "Content-Type": "application/json",
    }

    counter = 0

    with open(csv_filepath, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",")
        existing_records = fetch_existing_records()

        if existing_records is None:
            print("Failed to fetch existing records. Aborting upload.")
            return

        for row in reader:
            counter += 1
            data = {"fields": transform_data(row)}

            # Check for duplicate records based on the "Name" field
            if data["fields"]["Name"] in existing_records:
                print(f"Skipping duplicate record: {data['fields']['Name']}")
                continue

            response = requests.post(
                AIRTABLE_API_URL, headers=headers, data=json.dumps(data)
            )

            if response.status_code != 200:
                print(f"Error in data upload: {response.text}")
            else:
                print(
                    f"{counter} Successfully uploaded record with id: {response.json()['id']}"
                )
