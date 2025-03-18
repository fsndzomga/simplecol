import os
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
    # Test writing and reading
    test_dir = tmp_path / "test_data"
    ColumnarWriter.write(str(test_dir), sample_data["columns"], sample_data["schema"])

    reader = ColumnarReader(str(test_dir))
    assert reader.num_rows == 3

    # Test column reading
    assert reader.read_column("id") == [1, 2, 3]
    assert reader.read_column("name") == ["Alice", "Bob", "Charlie"]
    assert reader.read_column("score") == [85.5, 92.3, 78.9]

def test_metadata(tmp_path, sample_data):
    test_dir = tmp_path / "test_meta"
    ColumnarWriter.write(str(test_dir), sample_data["columns"], sample_data["schema"])

    assert os.path.exists(os.path.join(test_dir, "_metadata.json"))
    assert os.path.exists(os.path.join(test_dir, "id.bin"))
    assert os.path.exists(os.path.join(test_dir, "name.bin"))
    assert os.path.exists(os.path.join(test_dir, "score.bin"))
