import os
from dotenv import load_dotenv
import json


def load_secrets():
    load_dotenv()

    secrets_dict = {
        "AIRTABLE_API_TOKEN": os.getenv("AIRTABLE_API_TOKEN"),
        "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY"),
        "EVENTBRITE_PRIVATE_TOKEN": os.getenv("EVENTBRITE_PRIVATE_TOKEN"),
        "XANO_API_KEY": os.getenv("XANO_API_KEY"),
        "XANO_ENDPOINT_POST": os.getenv("XANO_ENDPOINT_POST"),
        "XANO_ENDPOINT_IMAGE": os.getenv("XANO_ENDPOINT_IMAGE"),
        "XANO_ENDPOINT_GET_ALL": os.getenv("XANO_ENDPOINT_GET_ALL"),
    }

    return secrets_dict


def load_settings(settings_path):
    # Load API keys from .env file
    api_keys_dict = load_secrets()

    # Check if the settings file exists
    if not os.path.exists(settings_path):
        raise FileNotFoundError(f"Settings file not found: {settings_path}")

    # Load the settings dictionary from the .json file
    with open(settings_path, "r") as fp:
        settings_dict = json.load(fp)

    # Merge the API keys and other settings
    settings_dict.update(api_keys_dict)

    return settings_dict
