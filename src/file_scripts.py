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


def manipulate_csv_data(file_path, output_filepath, operations, input_df=None):
    """
    This is how to set parameters:

    operations = [
            # ... other operations
            {'action': 'substring', 'column_name': 'Month', 'start_index': 0, 'end_index': 3},
            {'action': 'uppercase', 'column_name': 'Month'},
            # ... other substring operations
        ]
    """

    if input_df is None:
        df = pd.read_csv(file_path)
    else:
        df = input_df

    if output_filepath == None:
        output_filepath = file_path

    if operations == None:
        return print("No operations specified. Skipping function...")

    # Apply operations
    for operation in operations:
        if operation["action"] == "add_column":
            df[operation["column_name"]] = operation["column_value"]
        elif operation["action"] == "remove_column":
            df.drop(columns=[operation["column_name"]], axis=1, inplace=True)
            if len(df.columns) == 0:
                df.columns = pd.RangeIndex(
                    len(df.columns)
                )  # Reset the columns data type
        elif operation["action"] == "lowercase":
            df[operation["column_name"]] = (
                df[operation["column_name"]].astype(str).str.lower()
            )
        elif operation["action"] == "uppercase":
            df[operation["column_name"]] = (
                df[operation["column_name"]].astype(str).str.upper()
            )
        elif operation["action"] == "titlecase":
            df[operation["column_name"]] = (
                df[operation["column_name"]].astype(str).str.title()
            )
        elif operation["action"] == "split":
            df[operation["new_column_name"]] = (
                df[operation["column_name"]]
                .astype(str)
                .str.split(pat=operation["delimiter"])
            )

        elif operation["action"] == "substring":
            start_index = operation["start_index"]
            end_index = operation["end_index"]
            new_column_name = operation.get("new_column_name", None)

            if new_column_name:
                df[new_column_name] = (
                    df[operation["column_name"]].astype(str).str[start_index:end_index]
                )
            else:
                df[operation["column_name"]] = (
                    df[operation["column_name"]].astype(str).str[start_index:end_index]
                )

        elif operation["action"] == "keyword_filter":
            keyword = operation["keyword"]
            deleted_rows = df[
                df[operation["column_name"]].str.contains(keyword, case=False)
            ]
            print(deleted_rows.iloc[:, operation["column_index"]])
            df = df[~df[operation["column_name"]].str.contains(keyword, case=False)]

        else:
            raise ValueError(f"Invalid action '{operation['action']}'")

    df.to_csv(output_filepath, index=False)

    return df


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
