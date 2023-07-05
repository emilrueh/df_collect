import logging
import pandas as pd
import json
import tempfile
import shutil
import os
from pathlib import Path
import uuid
import numpy as np
from langdetect import detect
import subprocess

logger = logging.getLogger("main_logger")


# general data work
def get_data_dir(base_path: str = None):
    """
    Returns the path to the 'data' directory.
    If the directory doesn't exist, it is created.
    """
    if base_path is None:
        base_path = Path().resolve()

    data_dir = base_path / "data"
    # data_dir.mkdir(exist_ok=True)

    return data_dir


# get prject tree of current git
def git_tree_as_string(repo_path="."):
    # Get list of files in repository
    result = subprocess.run(
        ["git", "ls-files"], capture_output=True, cwd=repo_path, text=True
    )
    files = result.stdout.split("\n")

    # Build and print directory tree
    tree = {}
    for file in files:
        path = file.split("/")
        node = tree
        for part in path:
            node = node.setdefault(part, {})
    print_tree(tree)


def print_tree(tree, indent=""):
    for name, node in tree.items():
        print(f"{indent}{name}")
        if isinstance(node, dict):
            print_tree(node, indent + "    ")


def backup_data(input_data, backup_directory, input_name=None):
    # Convert backup_directory to a Path object
    backup_directory = Path(backup_directory)
    # Create backup_directory if it doesn't exist
    backup_directory.mkdir(parents=True, exist_ok=True)

    # Determine the file extension
    if isinstance(input_data, pd.DataFrame):
        file_extension = ".csv"
    elif isinstance(input_data, str) and input_data.endswith((".csv", ".txt", ".json")):
        file_extension = os.path.splitext(input_data)[1]
    elif isinstance(input_data, dict) or (
        isinstance(input_data, str) and input_data.endswith(".json")
    ):
        file_extension = ".json"
    elif isinstance(input_data, str):
        file_extension = ".txt"
    else:
        raise ValueError("Unsupported data type")

    # Create a temporary file with the desired file name
    with tempfile.NamedTemporaryFile(
        suffix=file_extension, delete=False, mode="w"
    ) as temp_file:
        # Save the input data to the temporary file
        if isinstance(input_data, pd.DataFrame):
            input_data.to_csv(temp_file.name, index=False)
        elif isinstance(input_data, str) and input_data.endswith(
            (".csv", ".txt", ".json")
        ):
            with open(input_data, "r") as data_file:
                temp_file.write(data_file.read())
        elif isinstance(input_data, dict):
            json.dump(input_data, temp_file)
        elif isinstance(input_data, str) and input_data.endswith(".json"):
            with open(input_data, "r") as data_file:
                json_data = json.load(data_file)
            with open(temp_file.name, "w") as temp_json_file:
                json.dump(json_data, temp_json_file)
        elif isinstance(input_data, str):
            temp_file.write(input_data.encode("utf-8"))

        # Determine the backup file name
        if input_name is not None:
            backup_file_name = f"{input_name}_backup{file_extension}"
        else:
            backup_file_name = f"backup_{uuid.uuid4()}{file_extension}"

        # Copy the temporary file to the backup directory
        backup_file_path = backup_directory / backup_file_name
        shutil.copy(temp_file.name, backup_file_path)

    # Remove the temporary file
    os.unlink(temp_file.name)


# JSON
def json_save(data, filename):
    # backup
    json_data = json.dumps(data)
    # Save the JSON string to a file
    with open(filename, "w") as file:
        file.write(json_data)


def json_read(json_filename):
    # Specify the filename of the JSON backup file
    # Load JSON data from the file
    with open(json_filename, "r") as file:
        json_data = file.read()

    # Convert the JSON data back into the dictionary
    json_dict = json.loads(json_data)

    return json_dict


def flatten_data(data):
    flattened_data = []
    for keyword in data:
        for item in data[keyword]:
            if data[keyword][item] is not None:
                temp_dict = data[keyword][item].copy()  # get the details by their ID
                flattened_data.append(temp_dict)
    return flattened_data


# DF and CSV
def json_to_df(file_path):
    # Load JSON data from file
    with open(file_path) as file:
        data = json.load(file)

    try:
        # Try to normalize the JSON data (i.e., handle nested structure)
        df = pd.json_normalize(data)
    except:
        # If an error is raised, assume the JSON data isn't nested
        df = pd.DataFrame(data)

    return df


def save_dict_to_csv(all_events_dict, csv_filename):
    flattened_data = []

    for keyword, keyword_events_dict in all_events_dict.items():
        for event_id, event_info in keyword_events_dict.items():
            if event_info is not None:  # if event_info is None, we skip the event
                info_copy = (
                    event_info.copy()
                )  # we don't want to modify the original dict
                info_copy["Keyword"] = keyword
                # info_copy["event_id"] = event_id
                flattened_data.append(info_copy)

    df = pd.DataFrame(flattened_data)
    df.to_csv(
        csv_filename, index=False
    )  # write dataframe to CSV, without the row index


def create_df(data):
    return pd.DataFrame(data)


def swap_columns(df, col1, col2):
    # Create new column order
    column_names = df.columns.tolist()
    i1, i2 = column_names.index(col1), column_names.index(col2)
    column_names[i2], column_names[i1] = column_names[i1], column_names[i2]

    # Reorder dataframe
    df = df[column_names]
    return df


def load_from_csv(filename):
    df = pd.read_csv(filename)
    return df


def delete_duplicates(data, columns_to_compare=None):
    if isinstance(data, str):  # if the input is a file path
        data = pd.read_csv(data)

    # Keep one instance of each event with the same name, remove others
    df_no_duplicates = data.drop_duplicates(subset=columns_to_compare, keep="first")

    return df_no_duplicates  # return the DataFrame object


def delete_duplicates_add_keywords(data, columns_to_compare=None):
    if isinstance(data, str):  # if the input is a file path
        data = pd.read_csv(data)

    original_data = data.copy()  # copy the original data to compare later

    # Convert 'Keyword' to a set, which removes duplicates within each group
    data["Keyword"] = data.groupby(columns_to_compare)["Keyword"].transform(
        lambda x: ",".join(set(x.str.split(",").sum()))
    )

    # Keep one instance of each event with the same name, remove others
    df_no_duplicates = data.drop_duplicates(subset=columns_to_compare, keep="first")

    # Print the indexes and keywords
    added_keywords_rows = df_no_duplicates[
        df_no_duplicates["Keyword"].str.contains(",", na=False)
    ]
    indexes_and_keywords = added_keywords_rows[["Keyword"]]
    logger.info("Rows that gained keywords:")
    with pd.option_context("display.max_rows", None, "display.max_columns", None):
        logger.info(indexes_and_keywords)
    logger.info(f"Total rows that gained keywords: {indexes_and_keywords.shape[0]}")

    return df_no_duplicates


def map_keywords_to_categories(keywords, category_dict):
    if pd.isnull(keywords):  # Add this check to handle NaN values (which are floats)
        return ""
    categories = set()  # Change this from a list to a set
    for keyword in keywords.split(","):
        keyword = keyword.strip().upper()  # Ensure format matches keys in dictionary
        if keyword in category_dict:
            category_values = category_dict[keyword]
            if isinstance(category_values, list):  # Check if the value is a list
                for value in category_values:
                    categories.add(value)
            else:
                categories.add(
                    category_values
                )  # Use add method for sets instead of append
    return ",".join(categories)


def manipulate_csv_data(
    file_path=None, output_filepath=None, operations=None, input_df=None
):
    if file_path is None and input_df is None:
        logger.error("Either a file path or an input DataFrame must be provided.")
        raise ValueError("Either a file path or an input DataFrame must be provided.")
    elif file_path is not None and input_df is not None:
        logger.error("Only one of file path or input DataFrame should be provided.")
        raise ValueError("Only one of file path or input DataFrame should be provided.")

    if input_df is not None:
        df = input_df
    else:
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            raise FileNotFoundError(f"File not found: {file_path}")
        df = pd.read_csv(file_path)

    # Fill NA/NaN values differently for numeric and non-numeric columns
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    non_numeric_cols = df.select_dtypes(exclude=[np.number]).columns
    df[numeric_cols] = df[numeric_cols].fillna(0)
    df[non_numeric_cols] = df[non_numeric_cols].fillna("")
    if output_filepath == None:
        output_filepath = file_path

    if operations == None:
        logger.warning("No operations specified. Skipping function...")
        return

    # Apply operations
    for operation in operations:
        action = operation["action"]
        column_name = operation.get("column_name")

        if column_name and column_name not in df.columns:
            logger.warning(f"Column '{column_name}' not found in DataFrame.")
            continue

        try:
            if action == "add_column":
                df[operation["column_name"]] = operation["column_value"]
            elif action == "remove_column":
                df.drop(columns=[column_name], axis=1, inplace=True)
            elif action in ["lowercase", "uppercase", "titlecase"]:
                df[column_name] = df[column_name].astype(str)
                if action == "lowercase":
                    df[column_name] = df[column_name].str.lower()
                elif action == "uppercase":
                    df[column_name] = df[column_name].str.upper()
                else:  # titlecase
                    df[column_name] = df[column_name].str.title()
            elif action == "split":
                df[operation["new_column_name"]] = df[column_name].str.split(
                    pat=operation["delimiter"]
                )
            elif action == "substring":
                start_index = operation["start_index"]
                end_index = operation["end_index"]
                new_column_name = operation.get("new_column_name", None)
                if new_column_name:
                    df[new_column_name] = df[column_name].str[start_index:end_index]
                else:
                    df[column_name] = df[column_name].str[start_index:end_index]
            elif action == "replace_string":
                df[column_name] = df[column_name].replace(
                    operation["old_text"], operation["new_text"], regex=True
                )
            elif action == "filter_out_keywords":
                keywords = [keyword.lower() for keyword in operation["keywords"]]
                columns = operation["columns"]
                mask = np.logical_or.reduce(
                    [
                        df[column].str.lower().str.contains(keyword, na=False)
                        for keyword in keywords
                        for column in columns
                    ]
                )
                df = df[~mask]
            elif action == "language_filter":
                languages = operation["languages"]
                column = operation["column_name"]
                mask = df[column].apply(
                    lambda x: detect(x) in languages if x else False
                )
                df = df[mask]
            elif action == "filter_for_keywords":
                columns = operation["columns"]
                keywords = [kw.lower() for kw in operation["keywords"]]
                skip_columns = operation.get("skip_columns", [])
                mask = []
                for index, row in df.iterrows():
                    row_text = " ".join(
                        str(row[column]).lower()
                        for column in columns
                        if column not in skip_columns
                    )
                    if any(keyword in row_text for keyword in keywords):
                        mask.append(True)
                    else:
                        mask.append(False)
                df = df[mask]
            else:
                logger.error(f"Invalid action '{action}'")
                raise ValueError(f"Invalid action '{action}'")

        except Exception as e:
            logger.error(
                f"Error occurred during action '{action}' on column '{column_name}': {e}",
                exc_info=True,
            )
            continue

    try:
        df.to_csv(output_filepath, index=False)
    except Exception as e:
        logger.error(
            f"Error occurred while saving DataFrame to CSV: {e}", exc_info=True
        )

    return df


# def manipulate_csv_data(
#     file_path=None, output_filepath=None, operations=None, input_df=None
# ):
#     """
#     This is how to set parameters:

#     operations = [
#             # ... other operations
#             {'action': 'substring', 'column_name': 'Month', 'start_index': 0, 'end_index': 3},
#             {'action': 'uppercase', 'column_name': 'Month'},
#             # ... other substring operations
#         ]
#     """

#     if file_path is None and input_df is None:
#         raise ValueError("Either a file path or an input DataFrame must be provided.")
#     elif file_path is not None and input_df is not None:
#         raise ValueError("Only one of file path or input DataFrame should be provided.")

#     if input_df is not None:
#         df = input_df
#     else:
#         df = pd.read_csv(file_path)

#     # Fill NA/NaN values differently for numeric and non-numeric columns
#     numeric_cols = df.select_dtypes(include=[np.number]).columns
#     non_numeric_cols = df.select_dtypes(exclude=[np.number]).columns
#     df[numeric_cols] = df[numeric_cols].fillna(0)
#     df[non_numeric_cols] = df[non_numeric_cols].fillna("")
#     if output_filepath == None:
#         output_filepath = file_path

#     if operations == None:
#         logger.warning("No operations specified. Skipping function...")
#         return print("No operations specified. Skipping function...")

#     # Apply operations
#     for operation in operations:
#         if operation["action"] == "add_column":
#             df[operation["column_name"]] = operation["column_value"]
#         elif operation["action"] == "remove_column":
#             df.drop(columns=[operation["column_name"]], axis=1, inplace=True)
#         elif operation["action"] == "lowercase":
#             df[operation["column_name"]] = (
#                 df[operation["column_name"]].astype(str).str.lower()
#             )
#         elif operation["action"] == "uppercase":
#             df[operation["column_name"]] = (
#                 df[operation["column_name"]].astype(str).str.upper()
#             )
#         elif operation["action"] == "titlecase":
#             df[operation["column_name"]] = (
#                 df[operation["column_name"]].astype(str).str.title()
#             )
#         elif operation["action"] == "split":
#             df[operation["new_column_name"]] = (
#                 df[operation["column_name"]]
#                 .astype(str)
#                 .str.split(pat=operation["delimiter"])
#             )
#         elif operation["action"] == "substring":
#             start_index = operation["start_index"]
#             end_index = operation["end_index"]
#             new_column_name = operation.get("new_column_name", None)
#             if new_column_name:
#                 df[new_column_name] = (
#                     df[operation["column_name"]].astype(str).str[start_index:end_index]
#                 )
#             else:
#                 df[operation["column_name"]] = (
#                     df[operation["column_name"]].astype(str).str[start_index:end_index]
#                 )
#         elif operation["action"] == "replace_string":
#             df[operation["column_name"]] = df[operation["column_name"]].replace(
#                 operation["old_text"], operation["new_text"], regex=True
#             )

#         elif operation["action"] == "filter_out_keywords":
#             keywords = [keyword.lower() for keyword in operation["keywords"]]
#             columns = operation["columns"]

#             mask = np.logical_or.reduce(
#                 [
#                     df[column].str.lower().str.contains(keyword, na=False)
#                     for keyword in keywords
#                     for column in columns
#                 ]
#             )
#             df = df[~mask]

#         elif operation["action"] == "language_filter":
#             languages = operation["languages"]
#             column = operation["column_name"]

#             mask = df[column].apply(lambda x: detect(x) in languages if x else False)
#             df = df[mask]

#         elif operation["action"] == "filter_for_keywords":
#             columns = operation["columns"]
#             keywords = [kw.lower() for kw in operation["keywords"]]
#             skip_columns = operation.get("skip_columns", [])

#             mask = []
#             # Process each row
#             for index, row in df.iterrows():
#                 row_text = ""

#                 # Combine text from all relevant columns
#                 for column in columns:
#                     if column not in skip_columns:
#                         row_text += " " + str(row[column]).lower()

#                 # Check if any of the keywords is in the combined text
#                 if any(keyword in row_text for keyword in keywords):
#                     mask.append(True)
#                 else:
#                     mask.append(False)

#             df = df[mask]

#         else:
#             logger.error(f"Invalid action '{operation['action']}'")
#             raise ValueError(f"Invalid action '{operation['action']}'")

#     try:
#         df.to_csv(output_filepath, index=False)
#     except Exception as e:
#         logger.error(f"Error occurred while saving DataFrame to CSV: {e}", exc_info=True)

#     return df
