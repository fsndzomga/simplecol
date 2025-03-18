import os
import json
import struct
import pytest
from lib.core import ColumnarWriter, ColumnarReader

@pytest.fixture
def sample_data():
    return {
        "columns": {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "score": [85.5, 92.3, 78.9]
        },
        "schema": {
            "id": "int",
            "name": "string",
            "score": "float"
        }
    }

def test_write_read(tmp_path, sample_data):
    # Write to a single .col file
    test_file = tmp_path / "test.col"
    ColumnarWriter.write(str(test_file), sample_data["columns"], sample_data["schema"])

    # Verify file existence
    assert os.path.exists(test_file)

    # Read back data
    reader = ColumnarReader(str(test_file))
    assert reader.num_rows == 3

    # Validate all columns
    assert reader.read_column("id") == [1, 2, 3]
    assert reader.read_column("name") == ["Alice", "Bob", "Charlie"]
    assert reader.read_column("score") == pytest.approx([85.5, 92.3, 78.9])

def test_metadata_structure(tmp_path, sample_data):
    test_file = tmp_path / "meta_test.col"
    ColumnarWriter.write(str(test_file), sample_data["columns"], sample_data["schema"])

    # Directly inspect the binary file structure
    with open(test_file, "rb") as f:
        # Read metadata length header
        metadata_length = struct.unpack("<I", f.read(4))[0]

        # Read and parse metadata
        metadata_json = json.loads(f.read(metadata_length))

        # Validate metadata content
        assert metadata_json["num_rows"] == 3
        assert metadata_json["schema"] == sample_data["schema"]

        # Check column entries in metadata
        assert {col["name"] for col in metadata_json["columns"]} == {"id", "name", "score"}
        for col in metadata_json["columns"]:
            assert col["type"] == sample_data["schema"][col["name"]]
            assert "length" in col  # Verify binary data length is stored

        # Verify total data size matches expectations
        expected_data_size = sum(col["length"] for col in metadata_json["columns"])
        actual_data_size = os.path.getsize(test_file) - (4 + metadata_length)
        assert actual_data_size == expected_data_size
