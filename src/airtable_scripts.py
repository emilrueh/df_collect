# airtable imports
import requests
import json
import csv


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


def fetch_existing_records(airtable_api_token, airtable_api_url):
    headers = {
        "Authorization": "Bearer " + airtable_api_token,
        "Content-Type": "application/json",
    }

    existing_records = {}
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
            response = requests.get(airtable_api_url, headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()
                records = data.get("records", [])
                if not records:  # No existing records
                    break
                existing_records.update(
                    {record["fields"]["Name"]: record for record in records}
                )
                if "offset" in data:
                    offset = data["offset"]
                else:
                    break
            else:
                print(f"Error fetching existing records: {response.text}")
                return {}  # Return an empty dictionary
    except Exception as e:
        print(f"Error fetching existing records: {str(e)}")
        return {}  # Return an empty dictionary

    return existing_records


def csv_to_airtable(airtable_api_token, airtable_api_url, csv_filepath):
    headers = {
        "Authorization": "Bearer " + airtable_api_token,
        "Content-Type": "application/json",
    }

    counter = 0

    with open(csv_filepath, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",")
        # function call
        existing_records = fetch_existing_records(airtable_api_token, airtable_api_url)

        if existing_records is None:
            print("Failed to fetch existing records. Aborting upload.")
            return

        for row in reader:
            counter += 1
            transformed_row = transform_data(row)
            data = {"fields": transformed_row}

            # Check for existing records based on the "Link" field
            existing_record = existing_records.get(data["fields"]["Link"])

            if existing_record:
                # Only update existing record if there are changes
                if transformed_row != existing_record["fields"]:
                    response = requests.patch(
                        f"{airtable_api_url}/{existing_record['id']}",
                        headers=headers,
                        data=json.dumps(data),
                    )
                    print(f"Updating record: {data['fields']['Link']}")
                else:
                    print(f"Skipping record with no changes: {data['fields']['Link']}")
                    continue
            else:
                # Create new record
                response = requests.post(
                    airtable_api_url, headers=headers, data=json.dumps(data)
                )
                print(f"Creating new record: {data['fields']['Name']}")

            if response.status_code != 200:
                print(f"Error in data upload: {response.text}")
            else:
                print(
                    f"\n{counter} Successfully uploaded record with id: {response.json()['id']}"
                )


def update_only_specified_columns(
    airtable_api_token, airtable_api_url, csv_filepath, columns_to_update
):
    headers = {
        "Authorization": "Bearer " + airtable_api_token,
        "Content-Type": "application/json",
    }

    counter = 0

    with open(csv_filepath, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",")
        # function call
        existing_records = fetch_existing_records(airtable_api_token, airtable_api_url)

        if existing_records is None:
            print("Failed to fetch existing records. Aborting upload.")
            return

        for row in reader:
            counter += 1
            transformed_row = transform_data(row)
            data = {"fields": transformed_row}

            # Check for existing records based on the "Link" field
            existing_record = existing_records.get(data["fields"]["Link"])

            if existing_record:
                # Check if specified columns need an update
                need_update = False
                update_data = {"fields": {}}
                for column in columns_to_update:
                    if existing_record["fields"].get(column) != data["fields"].get(
                        column
                    ):
                        need_update = True
                        update_data["fields"][column] = data["fields"][
                            column
                        ]  # Only update this column in the update data

                if need_update:
                    response = requests.patch(
                        f"{airtable_api_url}/{existing_record['id']}",
                        headers=headers,
                        data=json.dumps(update_data),
                    )
                    print(f"Updating record: {data['fields']['Link']}")
                else:
                    print(f"Skipping record with no changes: {data['fields']['Link']}")
            else:
                # Create new record
                response = requests.post(
                    airtable_api_url, headers=headers, data=json.dumps(data)
                )
                print(f"Creating new record: {data['fields']['Name']}")

            if response.status_code != 200:
                print(f"Error in data upload: {response.text}")
            else:
                print(
                    f"\n{counter} Successfully uploaded record with id: {response.json()['id']}"
                )
