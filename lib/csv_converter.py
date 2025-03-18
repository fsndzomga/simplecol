import csv
from typing import List
from .core import ColumnarWriter

def infer_type(values: List[str]) -> str:
    for value in values:
        try:
            int(value)
        except ValueError:
            break
    else:
        return 'int'

    for value in values:
        try:
            float(value)
        except ValueError:
            break
    else:
        return 'float'

    return 'string'

def csv_to_columnar(csv_path: str, output_dir: str):
    with open(csv_path, 'r') as f:
        reader = csv.reader(f)
        headers = next(reader)
        rows = list(reader)

    columns = {header: [] for header in headers}
    for row in rows:
        for header, value in zip(headers, row):
            columns[header].append(value)

    schema = {}
    for header in headers:
        schema[header] = infer_type(columns[header])

    typed_columns = {}
    for header in headers:
        col_type = schema[header]
        if col_type == 'int':
            typed_columns[header] = [int(v) for v in columns[header]]
        elif col_type == 'float':
            typed_columns[header] = [float(v) for v in columns[header]]
        else:
            typed_columns[header] = columns[header]

    ColumnarWriter.write(output_dir, typed_columns, schema)
