import os
from lib.core import ColumnarReader
from lib.csv_converter import csv_to_columnar, infer_type

def test_infer_type():
    assert infer_type(["1", "2", "3"]) == "int"
    assert infer_type(["1.1", "2.5", "3.0"]) == "float"
    assert infer_type(["apple", "123", "banana"]) == "string"
    assert infer_type(["2023-01-01", "2023-02-15", "2023-03-30"]) == "string"

def test_csv_conversion(tmp_path):
    # Get path to sample CSV
    csv_path = os.path.join(os.path.dirname(__file__), "..", "data", "sample.csv")
    output_dir = tmp_path / "converted"


    # Convert CSV
    csv_to_columnar(csv_path, str(output_dir))

    # Verify conversion
    reader = ColumnarReader(str(output_dir))
    assert reader.num_rows == 7
    assert [c["name"] for c in reader.metadata["columns"]] == [
        "id", "name", "age", "salary", "department"
    ]

    # Test type inference
    schema = {c["name"]: c["type"] for c in reader.metadata["columns"]}
    assert schema["id"] == "int"
    assert schema["age"] == "int"
    assert schema["salary"] == "float"
    assert schema["name"] == "string"
