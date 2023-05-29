from unittest.mock import patch, MagicMock
import pandas as pd
import pytest

from src.openai_scripts import call_openai, openai_loop_over_column_and_add


@patch("openai.ChatCompletion.create")
def test_call_openai(mock_create):
    """
    Test the 'call_openai' function to ensure it processes the API response correctly.
    Mocks the 'openai.ChatCompletion.create' function to return a controlled response.
    """
    # ARRANGE
    mock_response = MagicMock()
    mock_response.choices[0].message.content = "Mocked AI Response"
    mock_create.return_value = mock_response

    # ACT
    result = call_openai("fake_api_key", "fake_prompt", "fake_input_text")

    # ASSERT
    assert result == "Mocked AI Response"
    mock_create.assert_called_once_with(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": "fake_promptfake_input_text"},
        ],
    )


@patch("src.openai_scripts.call_openai")
@patch("pandas.DataFrame.to_csv")
def test_openai_loop_over_column_and_add(mock_to_csv, mock_call_openai):
    """
    Test the 'openai_loop_over_column_and_add' function to ensure it correctly adds AI responses
    to the output column of the DataFrame and writes to CSV.
    Mocks 'call_openai' function to return a controlled response, and 'to_csv' to avoid writing a file.
    """
    # ARRANGE
    df = pd.DataFrame(
        {"column_for_input": ["input1", "input2"], "column_for_output": ["", ""]}
    )
    mock_call_openai.return_value = "Mocked AI Response"

    # ACT
    result_df = openai_loop_over_column_and_add(
        "fake_api_key",
        "fake_prompt",
        df,
        "column_for_input",
        "column_for_output",
        "fake_path_to_csv",
    )

    # ASSERT
    expected_df = pd.DataFrame(
        {
            "column_for_input": ["input1", "input2"],
            "column_for_output": ["Mocked AI Response", "Mocked AI Response"],
        }
    )
    pd.testing.assert_frame_equal(result_df, expected_df)  # assert
    mock_to_csv.assert_called_once_with("fake_path_to_csv", index=False)  # assert
