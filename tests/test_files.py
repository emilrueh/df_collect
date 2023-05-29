import os
import pandas as pd
import pytest

from src.file_scripts import delete_csv_duplicates


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
