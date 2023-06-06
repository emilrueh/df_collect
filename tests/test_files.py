import os
import pandas as pd
import pytest

from src.data_scripts import delete_csv_duplicates, manipulate_csv_data


# file_scripts.py
@pytest.mark.parametrize(
    "input_file, columns_to_compare",
    [
        (
            "data\working_data\Events_Testing_Env.csv",
            ["Name", "Date", "Time", "Location"],
        ),
        # Add more test cases if needed
    ],
)
def test_delete_csv_duplicates(input_file, columns_to_compare):
    # Arrange
    # # Read the input file
    input_data = pd.read_csv(input_file)

    # Act
    # # Call the function
    delete_csv_duplicates(file_path=input_file, columns_to_compare=columns_to_compare)

    # # Read the output file
    output_file_path = input_file.replace(".csv", "_no-duplicates.csv")
    output_data = pd.read_csv(output_file_path)

    # Create a DataFrame that contains only those rows that are in input_data but not in output_data
    removed_data = pd.concat([input_data, output_data, output_data]).drop_duplicates(
        keep=False
    )

    # Print the rows that were removed
    print("Rows that were removed:")
    print(removed_data)

    # Assert
    # # Check that the name of the output file is correct
    print(f"PRINT: {output_file_path}")
    assert output_file_path == input_file.replace(".csv", "_no-duplicates.csv")

    # # Check if the input file is not empty
    assert len(input_data) > 0

    # # Check if the number of rows in the output file is less than the input file
    assert len(input_data) > len(output_data)

    # # Check if there are no duplicate rows in the output file based on the columns specified
    assert not output_data.duplicated(subset=columns_to_compare).any()

    # Clean (the temporary file)
    os.remove(output_file_path)


@pytest.mark.parametrize(
    "operations, expected_df",
    [
        (
            [{"action": "add_column", "column_name": "NewColumn", "column_value": 42}],
            pd.DataFrame({"NewColumn": [42, 42, 42, 42, 42]}),
        ),
        (
            [
                {
                    "action": "add_column",
                    "column_name": "NewColumn",
                    "column_value": ["One", "Two", "Three", "Four", "Five"],
                },
                {"action": "lowercase", "column_name": "NewColumn"},
            ],
            pd.DataFrame({"NewColumn": ["one", "two", "three", "four", "five"]}),
        ),
        (
            [
                {
                    "action": "add_column",
                    "column_name": "NewColumn",
                    "column_value": ["One", "Two", "Three", "Four", "Five"],
                },
                {"action": "lowercase", "column_name": "NewColumn"},
            ],
            pd.DataFrame({"NewColumn": ["one", "two", "three", "four", "five"]}),
        ),
        (
            [
                {
                    "action": "add_column",
                    "column_name": "NewColumn",
                    "column_value": ["one", "two", "three", "four", "five"],
                },
                {"action": "uppercase", "column_name": "NewColumn"},
            ],
            pd.DataFrame({"NewColumn": ["ONE", "TWO", "THREE", "FOUR", "FIVE"]}),
        ),
    ],
)
def test_add_remove_and_modify_columns(operations, expected_df):
    # ARRANGE
    df = pd.DataFrame(index=range(5))  # DataFrame with 5 empty rows

    # ACT
    result_df = manipulate_csv_data("input.csv", None, operations, input_df=df)

    # ASSERT
    pd.testing.assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize(
    "operations, input_file, output_file, expected_output",
    [
        (
            [
                {
                    "action": "substring",
                    "column_name": "Month",
                    "start_index": 0,
                    "end_index": 3,
                }
            ],
            "input.csv",
            "output.csv",
            ["Jan", "Feb", "Mar", "Apr", "May"],
        ),
        (
            [
                {
                    "action": "substring",
                    "column_name": "Month",
                    "start_index": 1,
                    "end_index": 2,
                }
            ],
            "input.csv",
            "output.csv",
            ["a", "e", "a", "p", "a"],
        ),
    ],
)
def test_substring_operations(operations, input_file, output_file, expected_output):
    # ARRANGE
    df = pd.DataFrame({"Month": ["January", "February", "March", "April", "May"]})
    df.to_csv(input_file, index=False)

    # ACT
    manipulate_csv_data(input_file, output_file, operations)

    # ASSERT
    expected_df = pd.DataFrame({"Month": expected_output})
    assert pd.read_csv(output_file).equals(expected_df)
