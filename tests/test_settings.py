import os
import pytest
import json

from src.settings_scripts import set_scraper_settings


# settings_scripts.py
@pytest.mark.parametrize(
    "keywords, number_of_events_per_keyword, ai_prompt",
    [
        (["founder", "invest", "funding"], 3, "Some AI prompt"),
        # Add more test cases if needed
    ],
)
def test_set_scraper_settings(
    keywords, number_of_events_per_keyword, ai_prompt, tmpdir
):
    # Arrange
    # Use pytest's tmpdir fixture to create a temporary directory for this test
    old_cwd = os.getcwd()
    os.chdir(tmpdir)

    # Act
    # Call the function
    settings = set_scraper_settings(
        keywords=keywords,
        number_of_events_per_keyword=number_of_events_per_keyword,
        ai_prompt=ai_prompt,
        csv_operations=None,
    )

    # Assert
    # Check the returned settings dictionary
    assert settings["KEYWORDS"] == keywords
    assert settings["NUMBER_OF_EVENTS_PER_KEYWORD"] == number_of_events_per_keyword
    assert settings["AI_PROMPT"] == ai_prompt

    print(f'Keywords: {settings["KEYWORDS"]}')

    # Check the JSON file
    with open("data/settings.json", "r") as fp:
        saved_settings = json.load(fp)

    assert saved_settings == settings

    # Clean up (switch back to the old current working directory)
    os.chdir(old_cwd)
