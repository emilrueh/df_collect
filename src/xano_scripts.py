import json
import requests
import pandas as pd
import numpy as np

import datetime
import requests

import time

from io import BytesIO
from PIL import Image

import base64


def try_parsing_date(text):
    for fmt in ("%B %d, %Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.datetime.strptime(text, fmt)
        except ValueError:
            pass
    raise ValueError("no valid date format found")


# def convert_image_to_jpg(image_url):
#     response = requests.get(image_url)
#     if response.status_code != 200:
#         raise Exception(f"Failed to download image from {image_url}")


#     img = Image.open(BytesIO(response.content))
#     with BytesIO() as output:
#         img.convert("RGB").save(output, "JPEG")
#         output.seek(0)
#         return output.read()
def convert_image_to_jpg(image_url):
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


# def transform_data(event_data):
#     transformed = {}
#     for k, v in event_data.items():
#         if k in ["Date"]:  # timestamp fields
#             date = try_parsing_date(v) if v else None
#             transformed[k] = date.isoformat() if date else None
#         else:  # all other fields
#             transformed[k] = v
#     return transformed


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


# print(f"URL 1: {image_url}")  # TEST PRINT !!!


def send_data_to_xano(
    data, api_key, image_endpoint_url, update_endpoint_url, get_all_endpoint_url
):
    if isinstance(data, pd.DataFrame):
        data = data.replace({np.nan: None})
        print("Converting DataFrame to JSON...")
        data = data.to_dict(orient="records")

    response_list = []

    headers = {
        "Authorization": f"Bearer {api_key}",
    }

    time.sleep(1)
    response = requests.get(get_all_endpoint_url, headers=headers)
    if response.status_code != 200:
        print("Failed to retrieve data from the database.")
        raise Exception(
            f"Request failed with status {response.status_code}. Message: {response.text}"
        )
    existing_entries = response.json()
    existing_links = {entry["Link"] for entry in existing_entries}

    for i, row in enumerate(data):
        time.sleep(4)
        transformed_row = transform_data(row)

        if transformed_row["Link"] in existing_links:
            print(f"Row {i + 1} already present in the database. Skipping...")
            continue

        if "Photo" in transformed_row and transformed_row["Photo"]:
            base64_image = transformed_row["Photo"]
            response = requests.post(
                image_endpoint_url, headers=headers, json={"content": base64_image}
            )

            if response.status_code != 200:
                print("Request failed.")
                raise Exception(
                    f"Request failed with status {response.status_code}. Message: {response.text}"
                )

            image_metadata = response.json()
            # print("DATA: ", image_metadata)
            transformed_row["Photo"] = image_metadata

        # Convert the row to JSON and send it to the update endpoint
        row_json = json.dumps(transformed_row)
        headers.update(
            {"Content-Type": "application/json"}
        )  # Add Content-Type to headers

        for attempt in range(3):  # Retry up to 3 times
            try:
                print(
                    f"\n{f'Attempt {attempt+1} ' if attempt > 0 else ''}Sending row {i + 1} to Xano..."
                )
                print(transformed_row["Link"])
                response = requests.post(
                    update_endpoint_url, headers=headers, data=row_json
                )
                response.raise_for_status()  # Raises a HTTPError if the status is 4xx, 5xx
            except requests.exceptions.HTTPError as e:
                wait_time = 2**attempt  # Exponential backoff
                print(
                    f"Attempt {attempt+1} failed with error: {e}. Retrying in {wait_time} seconds..."
                )
                time.sleep(wait_time)
                if attempt == 2:  # If this was the last attempt
                    print("All attempts failed.")
                    raise  # Re-raise the last exception
            else:  # If no exception was raised in the try block
                break  # Exit the loop

        print(f"Data sent successfully for row {i + 1}.")
        response_json = response.json()
        response_list.append(response_json)

    return response_list


# def send_data_to_xano(
#     data, api_key, image_endpoint_url, update_endpoint_url, get_all_endpoint_url
# ):
#     # If the data is a DataFrame, convert it to a list of dictionaries
#     if isinstance(data, pd.DataFrame):
#         data = data.replace({np.nan: None})
#         print("Converting DataFrame to JSON...")
#         data = data.to_dict(orient="records")

#     response_list = []

#     # Headers for requests
#     headers = {
#         "Authorization": f"Bearer {api_key}",
#     }

#     # Retrieve all the existing entries from the database
#     time.sleep(1)
#     response = requests.get(get_all_endpoint_url, headers=headers)
#     if response.status_code != 200:
#         print("Failed to retrieve data from the database.")
#         raise Exception(
#             f"Request failed with status {response.status_code}. Message: {response.text}"
#         )
#     existing_entries = response.json()
#     existing_links = {entry["Link"] for entry in existing_entries}

#     # Iterate over each data row (dictionary)
#     for i, row in enumerate(data):
#         time.sleep(4)
#         # Transform the data row
#         transformed_row = transform_data(row)

#         # Check if the row is already present in the database
#         if transformed_row["Link"] in existing_links:
#             print(f"Row {i + 1} already present in the database. Skipping...")
#             continue

#         # Check if there's a base64 encoded image string in the "Photo" field
#         if "Photo" in transformed_row and transformed_row["Photo"]:
#             base64_image = transformed_row["Photo"]
#             # print("Base64 Image:", base64_image)

#             # Send the base64 encoded image string to the image_endpoint
#             response = requests.post(
#                 image_endpoint_url, headers=headers, json={"content": base64_image}
#             )

#             if response.status_code != 200:
#                 print("Request failed.")
#                 raise Exception(
#                     f"Request failed with status {response.status_code}. Message: {response.text}"
#                 )

#             # Update the "Photo" field with the generated metadata
#             image_metadata = response.json()
#             print("DATA: ", image_metadata)
#             transformed_row["Photo"] = image_metadata

#         # Convert the row to JSON and send it to the update endpoint
#         row_json = json.dumps(transformed_row)
#         print(f"\nSending row {i + 1} to Xano...")
#         print(row_json["Link"])  # TEST PRINT CHECKING CONTENT
#         headers.update(
#             {"Content-Type": "application/json"}
#         )  # Add Content-Type to headers
#         response = requests.post(update_endpoint_url, headers=headers, data=row_json)

#         if response.status_code != 200:
#             print("Request failed.")
#             raise Exception(
#                 f"Request failed with status {response.status_code}. Message: {response.text}"
#             )

#         print(f"Data sent successfully for row {i + 1}.")
#         response_json = response.json()
#         response_list.append(response_json)

#     return response_list
