import openai
import time
import requests


def call_openai(prompt, input_text):
    # Set your OpenAI API key
    openai.api_key = "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"

    # Concatenate the prompt and input input_text
    full_prompt = prompt + input_text

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

            print(f"Summary: {output_text}")

            return output_text

        except (openai.error.RateLimitError, requests.exceptions.ConnectionError):
            print(f"Error encountered. Waiting for {(2**attempts)} seconds.")
            time.sleep((2**attempts))
            attempts += 1
