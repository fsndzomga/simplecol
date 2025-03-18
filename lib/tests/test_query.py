import pytest
import os
from lib.core import ColumnarReader
from lib.query import QueryEngine

@pytest.fixture
def test_engine(tmp_path):
    # Convert sample CSV first
    from lib.csv_converter import csv_to_columnar
    csv_path = os.path.join(os.path.dirname(__file__), "..", "data", "sample.csv")
    output_dir = tmp_path / "query_test"
    csv_to_columnar(csv_path, str(output_dir))

    reader = ColumnarReader(str(output_dir))
    return QueryEngine(reader)

def test_basic_query(test_engine):
    results = test_engine.execute("SELECT name, age FROM data")
    assert len(results) == 7
    assert results[0]["name"] == "Alice"
    assert results[0]["age"] == 28

def test_where_clause(test_engine):
    results = test_engine.execute("SELECT name FROM data WHERE age > 35")
    assert len(results) == 3
    names = {r["name"] for r in results}
    assert names == {"Charlie", "Evan", "George"}

def test_multiple_columns(test_engine):
    results = test_engine.execute("SELECT department, salary FROM data WHERE department = 'Engineering'")
    assert len(results) == 2
    assert {r["department"] for r in results} == {"Engineering"}
    assert sum(r["salary"] for r in results) == 75000.50 + 68000.00
