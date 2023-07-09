import logging
from src.helper_utils import main_logger_name

logger_name = main_logger_name
logger = logging.getLogger(logger_name)


import time
import requests
import pandas as pd

import openai


def call_openai(api_key, prompt, input_text):
    # Set your OpenAI API key
    openai.api_key = api_key

    # Concatenate the prompt and input input_text
    full_prompt = prompt + str(input_text)

    attempts = 0
    while attempts < 5:
        try:
            # Send the request to the OpenAI API
            response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": full_prompt},
                ],
            )

            # Extract the generated summary from the API response
            output_text = response.choices[0].message.content

            # print(f"\n{output_text}")

            # Remove non-ASCII characters from the output_text
            output_text = output_text.encode("ascii", "ignore").decode()

            return output_text

        except (
            openai.error.RateLimitError,
            requests.exceptions.ConnectionError,
            openai.error.APIError,
            openai.error.ServiceUnavailableError,
            openai.error.APIConnectionError,
        ):
            logger.warning(
                f"ERROR encountered. New API call attempt in {(2**attempts)} seconds..."
            )
            logger.debug("Exception details:", exc_info=True)
            time.sleep((2**attempts))
            attempts += 1


def openai_loop_over_column_and_add(
    api_key,
    prompt,
    data,
    column_for_input,
    column_for_output,
    path_to_file=None,
    char_max=None,
    char_min=None,
    to_remove=None,
):
    if char_max is not None and char_min is None:
        char_min = int(0.4 * char_max)

    # Convert data to a uniform format (list of dictionaries)
    original_type = type(data)
    if isinstance(data, pd.DataFrame):
        data = data.to_dict(orient="records")
    elif isinstance(data, list):
        data = [{"data": item} for item in data]

    for i, row in enumerate(data):
        api_output = call_openai(api_key, prompt, row[column_for_input])

        while (char_max and len(api_output) > char_max) or (
            char_min and len(api_output) < char_min
        ):
            logger.warning(
                f"Output length not within limits {char_min} and {char_max} with {len(api_output)} characters at row {i}. Trying again..."
            )
            time.sleep(0.5)
            api_output = call_openai(api_key, prompt, row[column_for_input])

            logger.info(f"Retry: | Index: {i} | {api_output}")

        if to_remove is not None:
            for string in to_remove:
                api_output = api_output.replace(string, "")

        row[column_for_output] = api_output
        logger.info(f"Event: {i + 1} | Index: {i} | {row[column_for_output]}")

    if path_to_file is not None:
        path_to_file = (
            path_to_file.rsplit(".", 1)[0] + "_SUM." + path_to_file.rsplit(".", 1)[1]
        )

        # Convert back to DataFrame if necessary
        if isinstance(data, list):
            data = pd.DataFrame(data)

        data.to_csv(path_to_file, index=False)
        logger.info(f"New file saved at path: {path_to_file}")

    return data
