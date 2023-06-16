import openai
import time
import requests


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
        ):
            print(
                f"ERROR encountered. New API call attempt in {(2**attempts)} seconds...\n"
            )
            time.sleep((2**attempts))
            attempts += 1


# function to loop over every field of a column and call the openai api to add to the another specified column
def openai_loop_over_column_and_add(
    api_key, prompt, df, column_for_input, column_for_output, path_to_csv
):
    row_counter = 0
    for index, row in df.iterrows():
        row_counter += 1
        df.loc[index, column_for_output] = call_openai(
            api_key,
            prompt,
            row[column_for_input],
        )
        print(
            f"\nEvent: {row_counter} | Index: {index} | {df.loc[index, column_for_output]}"
        )

    # adjust the saving of the new df to csv by adding '_SUM' just before the file ending
    path_to_csv = (
        path_to_csv.rsplit(".", 1)[0] + "_SUM." + path_to_csv.rsplit(".", 1)[1]
    )

    # save new df to csv
    df.to_csv(path_to_csv, index=False)

    print(f"New csv saved at path: {path_to_csv}")

    return df
