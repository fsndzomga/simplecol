import os
import pytest
from lib.core import ColumnarReader
from lib.csv_converter import csv_to_columnar, infer_type

def test_infer_type():
    # Test type inference functionality
    assert infer_type(["1", "2", "3"]) == "int"
    assert infer_type(["1.1", "2.5", "3.0"]) == "float"
    assert infer_type(["apple", "123", "banana"]) == "string"
    assert infer_type(["2023-01-01", "2023-02-15", "2023-03-30"]) == "string"

def test_csv_conversion(tmp_path):
    # Get path to sample CSV
    csv_path = os.path.join(os.path.dirname(__file__), "..", "data", "sample.csv")

    # Define output path (use tmp_path for isolated testing)
    output_file = tmp_path / "sample.col"

    # Convert CSV to columnar format
    csv_to_columnar(csv_path, str(output_file))

    # Verify output file exists
    assert os.path.exists(output_file)

    # Read the columnar file
    reader = ColumnarReader(str(output_file))
    assert reader.num_rows == 7  # Verify number of rows

    # Verify column names
    assert [c["name"] for c in reader.metadata["columns"]] == [
        "id", "name", "age", "salary", "department"
    ]

    # Test type inference results
    schema = {c["name"]: c["type"] for c in reader.metadata["columns"]}
    assert schema["id"] == "int"
    assert schema["age"] == "int"
    assert schema["salary"] == "float"
    assert schema["name"] == "string"
    assert schema["department"] == "string"

    # Verify actual data
    assert reader.read_column("id") == [1, 2, 3, 4, 5, 6, 7]
    assert reader.read_column("name") == [
        "Alice", "Bob", "Charlie", "Diana", "Evan", "Fiona", "George"
    ]
    assert reader.read_column("age") == [28, 32, 45, 23, 38, 29, 41]
    assert reader.read_column("salary") == pytest.approx([75000.50, 82000.0, 95000.75, 68000.00, 105000.00, 72000.50, 88000.25])
    assert reader.read_column("department") == [
        "Engineering", "Sales", "Marketing", "Engineering", "HR", "Sales", "Marketing"
    ]
