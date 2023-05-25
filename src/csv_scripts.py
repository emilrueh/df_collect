# CSV imports
import csv
import pandas as pd


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
