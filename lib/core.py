import json
import struct
from typing import Dict, List, Any

class ColumnarWriter:
    @staticmethod
    def write(filename: str, columns: Dict[str, List[Any]], schema: Dict[str, str]):
        # Check all columns have the same number of rows
        num_rows = len(next(iter(columns.values())))
        for col_data in columns.values():
            if len(col_data) != num_rows:
                raise ValueError("All columns must have the same number of rows")

        # Prepare binary data for each column and collect metadata
        column_binaries = []
        ordered_columns = []
        for col_name in columns:
            data = columns[col_name]
            col_type = schema[col_name]
            binary_data = bytearray()

            for value in data:
                if col_type == 'int':
                    binary_data += struct.pack('<i', value)
                elif col_type == 'float':
                    binary_data += struct.pack('<d', value)
                elif col_type == 'string':
                    encoded = value.encode('utf-8')
                    binary_data += struct.pack('<I', len(encoded))
                    binary_data += encoded
                else:
                    raise ValueError(f"Unsupported type: {col_type}")

            column_binaries.append(bytes(binary_data))
            ordered_columns.append({
                "name": col_name,
                "type": col_type,
                "length": len(binary_data)
            })

        # Prepare metadata
        metadata = {
            "columns": ordered_columns,
            "num_rows": num_rows,
            "schema": schema
        }

        # Convert metadata to JSON and compute its length
        metadata_json = json.dumps(metadata).encode('utf-8')
        metadata_length = len(metadata_json)

        # Write to the .col file
        with open(filename, 'wb') as f:
            # Write metadata length (4 bytes, unsigned int)
            f.write(struct.pack('<I', metadata_length))
            # Write metadata JSON
            f.write(metadata_json)
            # Write each column's binary data in order
            for binary in column_binaries:
                f.write(binary)

class ColumnarReader:
    def __init__(self, filename: str):
        self.filename = filename
        with open(filename, 'rb') as f:
            # Read metadata length
            metadata_length_bytes = f.read(4)
            if len(metadata_length_bytes) != 4:
                raise ValueError("Invalid file format: metadata length missing")
            metadata_length = struct.unpack('<I', metadata_length_bytes)[0]

            # Read metadata JSON
            metadata_json = f.read(metadata_length)
            if len(metadata_json) != metadata_length:
                raise ValueError("Invalid file format: incomplete metadata")
            self.metadata = json.loads(metadata_json)

            # Calculate start position of column data
            self.columns_start = 4 + metadata_length

        self.num_rows = self.metadata['num_rows']

        # Precompute column offsets
        self.column_offsets = []
        current_offset = self.columns_start
        for col in self.metadata['columns']:
            self.column_offsets.append(current_offset)
            current_offset += col['length']

    def read_column(self, col_name: str) -> List[Any]:
        # Find the column's index and info
        col_index = None
        col_info = None
        for idx, col in enumerate(self.metadata['columns']):
            if col['name'] == col_name:
                col_index = idx
                col_info = col
                break
        if col_info is None:
            raise ValueError(f"Column '{col_name}' not found")

        col_type = col_info['type']
        col_length = col_info['length']
        col_offset = self.column_offsets[col_index]

        # Read the column's binary data
        with open(self.filename, 'rb') as f:
            f.seek(col_offset)
            data_bytes = f.read(col_length)

        data = []
        if col_type == 'int':
            num_values = col_length // 4
            fmt = '<' + ('i' * num_values)
            data = list(struct.unpack(fmt, data_bytes))
        elif col_type == 'float':
            num_values = col_length // 8
            fmt = '<' + ('d' * num_values)
            data = list(struct.unpack(fmt, data_bytes))
        elif col_type == 'string':
            ptr = 0
            while ptr < col_length:
                # Read string length
                if ptr + 4 > col_length:
                    break
                str_len = struct.unpack('<I', data_bytes[ptr:ptr+4])[0]
                ptr += 4
                # Read string
                if ptr + str_len > col_length:
                    break
                s = data_bytes[ptr:ptr+str_len].decode('utf-8')
                data.append(s)
                ptr += str_len
        else:
            raise ValueError(f"Unsupported column type: {col_type}")

        if len(data) != self.num_rows:
            raise ValueError(f"Column '{col_name}' has {len(data)} rows, expected {self.num_rows}")

        return data
