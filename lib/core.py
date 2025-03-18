import os
import json
import struct
from typing import Dict, List, Any

class ColumnarWriter:
    @staticmethod
    def write(directory: str, columns: Dict[str, List[Any]], schema: Dict[str, str]):
        os.makedirs(directory, exist_ok=True)
        metadata = {"columns": [], "num_rows": len(next(iter(columns.values())))}

        for col_name, data in columns.items():
            col_type = schema[col_name]
            filename = f"{col_name}.bin"
            metadata["columns"].append({
                "name": col_name,
                "type": col_type,
                "file": filename
            })

            with open(os.path.join(directory, filename), 'wb') as f:
                for value in data:
                    if col_type == 'int':
                        f.write(struct.pack('<i', value))
                    elif col_type == 'float':
                        f.write(struct.pack('<d', value))
                    elif col_type == 'string':
                        encoded = value.encode('utf-8')
                        f.write(struct.pack('<I', len(encoded)))
                        f.write(encoded)

        with open(os.path.join(directory, '_metadata.json'), 'w') as f:
            json.dump(metadata, f)

class ColumnarReader:
    def __init__(self, directory: str):
        self.directory = directory
        with open(os.path.join(directory, '_metadata.json'), 'r') as f:
            self.metadata = json.load(f)
        self.num_rows = self.metadata['num_rows']

    def read_column(self, col_name: str) -> List[Any]:
        col_info = next(c for c in self.metadata['columns'] if c['name'] == col_name)
        data = []
        with open(os.path.join(self.directory, col_info['file']), 'rb') as f:
            for _ in range(self.num_rows):
                if col_info['type'] == 'int':
                    data.append(struct.unpack('<i', f.read(4))[0])
                elif col_info['type'] == 'float':
                    data.append(struct.unpack('<d', f.read(8))[0])
                elif col_info['type'] == 'string':
                    str_len = struct.unpack('<I', f.read(4))[0]
                    data.append(f.read(str_len).decode('utf-8'))
        return data
