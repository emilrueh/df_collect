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

        except (openai.error.RateLimitError, requests.exceptions.ConnectionError):
            print(
                f"ERROR encountered. New API call attempt in {(2**attempts)} seconds...\n"
            )
            time.sleep((2**attempts))
            attempts += 1


# function to loop over every field of a column and call the openai api to add to the another specified column
def openai_loop_over_column_and_add(
    api_key, prompt, df, column_for_input, column_for_output, path_to_csv
):
    for index, row in df.iterrows():
        df.loc[index, column_for_output] = call_openai(
            api_key,
            prompt,
            row[column_for_input],
        )
        print(f"\n{index} | {df.loc[index, column_for_output]}")

    # save new df to csv
    df.to_csv(path_to_csv, index=False)

    print(f"New csv saved at path: {path_to_csv}")

    return df
