import zlib
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

            # Compress the binary data
            compressed_data = zlib.compress(bytes(binary_data))
            column_binaries.append(compressed_data)
            ordered_columns.append({
                "name": col_name,
                "type": col_type,
                "length": len(compressed_data),
                "original_length": len(binary_data),
                "compression": "zlib"
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
            # Write each column's compressed binary data in order
            for binary in column_binaries:
                f.write(binary)

class ColumnarReader:
    def __init__(self, filename: str):
        self.filename = filename
        with open(filename, 'rb') as f:
            # Read metadata length
            metadata_length = struct.unpack('<I', f.read(4))[0]
            # Read metadata JSON
            metadata_json = f.read(metadata_length)
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
        col_info = next(c for c in self.metadata['columns'] if c['name'] == col_name)
        col_type = col_info['type']
        col_length = col_info['length']
        col_offset = self.column_offsets[self.metadata['columns'].index(col_info)]

        # Read the column's compressed binary data
        with open(self.filename, 'rb') as f:
            f.seek(col_offset)
            compressed_data = f.read(col_length)

        # Decompress the data
        binary_data = zlib.decompress(compressed_data)

        # Parse the binary data
        data = []
        if col_type == 'int':
            num_values = col_info['original_length'] // 4
            fmt = '<' + ('i' * num_values)
            data = list(struct.unpack(fmt, binary_data))
        elif col_type == 'float':
            num_values = col_info['original_length'] // 8
            fmt = '<' + ('d' * num_values)
            data = list(struct.unpack(fmt, binary_data))
        elif col_type == 'string':
            ptr = 0
            while ptr < col_info['original_length']:
                str_len = struct.unpack('<I', binary_data[ptr:ptr+4])[0]
                ptr += 4
                s = binary_data[ptr:ptr+str_len].decode('utf-8')
                data.append(s)
                ptr += str_len
        else:
            raise ValueError(f"Unsupported column type: {col_type}")

        return data
