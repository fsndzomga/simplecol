import csv
from typing import List, Optional
from .core import ColumnarWriter
import os

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

def csv_to_columnar(csv_path: str, output_path: Optional[str] = None):
    """Convert CSV file to columnar format with automatic path handling"""
    # Read and parse CSV data
    with open(csv_path, 'r') as f:
        reader = csv.reader(f)
        headers = next(reader)
        rows = list(reader)

    # Organize data into columns
    columns = {header: [] for header in headers}
    for row in rows:
        for header, value in zip(headers, row):
            columns[header].append(value)

    # Infer schema from data
    schema = {}
    for header in headers:
        schema[header] = infer_type(columns[header])

    # Convert columns to appropriate types
    typed_columns = {}
    for header in headers:
        col_type = schema[header]
        try:
            if col_type == 'int':
                typed_columns[header] = [int(v) for v in columns[header]]
            elif col_type == 'float':
                typed_columns[header] = [float(v) for v in columns[header]]
            else:
                typed_columns[header] = columns[header]
        except ValueError:
            # Fallback to string type if conversion fails
            typed_columns[header] = columns[header]
            schema[header] = 'string'

    # Determine output filename
    base_name = os.path.splitext(os.path.basename(csv_path))[0]

    if output_path:
        # Check if output path is an existing directory
        if os.path.isdir(output_path):
            output_file = os.path.join(output_path, f"{base_name}.col")
        else:
            # Treat as file path, create parent directories if needed
            parent_dir = os.path.dirname(output_path)
            if parent_dir and not os.path.exists(parent_dir):
                os.makedirs(parent_dir, exist_ok=True)
            output_file = output_path if output_path.endswith('.col') else f"{output_path}.col"
    else:
        # Use current directory with default filename
        output_file = os.path.join(os.getcwd(), f"{base_name}.col")

    # Write to columnar format
    ColumnarWriter.write(output_file, typed_columns, schema)
