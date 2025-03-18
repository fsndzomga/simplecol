# SimpleCol - Columnar Storage System

## Installation
```bash
pip install simplecol
```

## Quick Start
```python
from simplecol import ColumnarWriter, csv_to_columnar, QueryEngine

# Convert CSV to columnar format
csv_to_columnar('input.csv', 'output.col')

# Query the data
reader = ColumnarReader('output.col')
engine = QueryEngine(reader)
results = engine.execute("SELECT name, age FROM data WHERE age > 25")
```
