import csv
import pandas as pd
import json


# to CSV function
def save_to_csv(data, filename):
    if not data:
        print("No data to save.")
        return

    fieldnames = [
        "Name",
        "Photo",
        "Category",
        "Tags",
        "People",
        "Group Size",
        "Long Description",
        "Summary",
        "Location",
        "Venue",
        "Gmaps link",
        "Date",
        "Time",
        "Price",
        "Link",
        "Keyword",
    ]

    flattened_data = []
    for events in data.values():
        for event_data in events.values():
            # Remove the URL field from the event_data dictionary
            event_data.pop("URL", None)
            flattened_data.append(event_data)

    with open(filename, "w", newline="", encoding="utf-8") as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        dict_writer.writeheader()
        dict_writer.writerows(flattened_data)


# from CSV function
def load_from_csv(filename):
    df = pd.read_csv(filename)
    return df


def delete_csv_duplicates(file_path, columns_to_compare=None):
    data = pd.read_csv(file_path)

    # Keep one instance of each event with the same name, remove others
    data_no_duplicates = data.drop_duplicates(subset=columns_to_compare, keep="first")

    data_no_duplicates.to_csv(
        f"{file_path.replace('.csv', '')}_no-duplicates.csv", index=False
    )


def json_read(json_filename):
    # Specify the filename of the JSON backup file
    # Load JSON data from the file
    with open(json_filename, "r") as file:
        json_data = file.read()

    # Convert the JSON data back into the dictionary
    json_dict = json.loads(json_data)

    return json_dict


def json_save(data, filename):
    # backup
    json_data = json.dumps(data)
    # Save the JSON string to a file
    with open(filename, "w") as file:
        file.write(json_data)
