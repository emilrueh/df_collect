import openai
import time
import requests
import json
import pandas as pd

from src.data_scripts import json_read


def call_openai(api_key, prompt, input_text):
    # Set your OpenAI API key
    openai.api_key = api_key

    # Concatenate the prompt and input input_text
    full_prompt = prompt + str(input_text)

    attempts = 0
    while attempts < 5:
        try:
            # Send the request to the Chat API
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

            return output_text

        except (
            openai.error.RateLimitError,
            requests.exceptions.ConnectionError,
            openai.error.APIError,
            openai.error.ServiceUnavailableError,
        ):
            print(
                f"ERROR encountered. New API call attempt in {(2**attempts)} seconds...\n"
            )
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
    row_counter = 0
    if char_max is not None and char_min is None:
        char_min = int(0.4 * char_max)

    if isinstance(data, pd.DataFrame):
        for index, row in data.iterrows():
            row_counter += 1
            api_output = call_openai(api_key, prompt, row[column_for_input])

            while (char_max and len(api_output) > char_max) or (
                char_min and len(api_output) < char_min
            ):
                print(
                    f"Output length not within limits {char_min} and {char_max} with {len(api_output)} characters at row {index}. Trying again..."
                )
                time.sleep(0.5)
                api_output = call_openai(api_key, prompt, row[column_for_input])

                print(f"\nRetry: | Index: {index}\n{api_output}")

            if to_remove is not None:
                for string in to_remove:
                    api_output = api_output.replace(string, "")

            data.loc[index, column_for_output] = api_output
            print(
                f"\nEvent: {row_counter} | Index: {index}\n{data.loc[index, column_for_output]}"
            )
    elif isinstance(data, list):
        for i, item in enumerate(data):
            row_counter += 1
            api_output = call_openai(api_key, prompt, item[column_for_input])

            while (char_max and len(api_output) > char_max) or (
                char_min and len(api_output) < char_min
            ):
                print(
                    f"Output length not within limits {char_min} and {char_max} with {len(api_output)} characters at item {i}. Trying again..."
                )
                time.sleep(0.5)
                api_output = call_openai(api_key, prompt, item[column_for_input])

                print(f"\nRetry: | Index: {i}\n{api_output}")

            if to_remove is not None:
                for string in to_remove:
                    api_output = api_output.replace(string, "")

            item[column_for_output] = api_output
            print(f"\nEvent: {row_counter} | Index: {i}\n{item[column_for_output]}")

    if path_to_file is not None:
        # adjust the saving of the new data to the file by adding '_SUM' just before the file ending
        path_to_file = (
            path_to_file.rsplit(".", 1)[0] + "_SUM." + path_to_file.rsplit(".", 1)[1]
        )

        # save new data to the file
        if isinstance(data, pd.DataFrame):
            data.to_csv(path_to_file, index=False)
        elif isinstance(data, list):
            with open(path_to_file, "w") as f:
                json.dump(data, f)

        print(f"New file saved at path: {path_to_file}")

    return data


# function to loop over every field of a column and call the openai api to add to the another specified column

# def openai_loop_over_column_and_add(
#     api_key,
#     prompt,
#     df,
#     column_for_input,
#     column_for_output,
#     path_to_csv,
#     char_max=None,
#     char_min=None,
# ):
#     row_counter = 0
#     if char_max is not None and char_min is None:
#         char_min = int(0.4 * char_max)

#     for index, row in df.iterrows():
#         row_counter += 1
#         api_output = call_openai(api_key, prompt, row[column_for_input])

#         # Call the API until output length is within the specified limits
#         while (char_max and len(api_output) > char_max) or (
#             char_min and len(api_output) < char_min
#         ):
#             print(
#                 f"Output length not within limits {char_min} and {char_max} with {len(api_output)} characters at row {index}. Trying again..."
#             )
#             time.sleep(0.5)
#             api_output = call_openai(api_key, prompt, row[column_for_input])

#             print(f"\nRetry: | Index: {index}\n{api_output}")

#         df.loc[index, column_for_output] = api_output
#         print(
#             f"\nEvent: {row_counter} | Index: {index}\n{df.loc[index, column_for_output]}"
#         )

#     # adjust the saving of the new df to csv by adding '_SUM' just before the file ending
#     path_to_csv = (
#         path_to_csv.rsplit(".", 1)[0] + "_SUM." + path_to_csv.rsplit(".", 1)[1]
#     )

#     # save new df to csv
#     df.to_csv(path_to_csv, index=False)

#     print(f"New csv saved at path: {path_to_csv}")

#     return df
