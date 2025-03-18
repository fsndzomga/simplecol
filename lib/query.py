from typing import List, Dict, Any
from .core import ColumnarReader
import re

class QueryEngine:
    def __init__(self, reader: ColumnarReader):
        self.reader = reader
        self.column_cache = {}

    def _get_column(self, col_name: str) -> List[Any]:
        if col_name not in self.column_cache:
            self.column_cache[col_name] = self.reader.read_column(col_name)
        return self.column_cache[col_name]

    def execute(self, query: str) -> List[Dict]:
        # Case-insensitive split for SQL keywords
        select_part, rest = re.split(r'\s+FROM\s+', query, 1, flags=re.IGNORECASE)
        from_part = rest.split(' WHERE ', 1)[0].strip() if ' WHERE ' in rest else rest

        where_clause = None
        if ' WHERE ' in rest.upper():  # Case-insensitive check
            _, where_part = re.split(r'\s+WHERE\s+', rest, 1, flags=re.IGNORECASE)
            where_clause = where_part.strip()

        # Keep original case for columns and values
        columns = [col.strip() for col in select_part.replace('SELECT ', '').split(',')]

        # Get all data
        data = {col: self._get_column(col) for col in columns}

        # Apply where clause
        if where_clause:
            col_name, op, value = self._parse_where(where_clause)
            col_data = self._get_column(col_name)
            col_type = next(c['type'] for c in self.reader.metadata['columns']
                         if c['name'] == col_name)
            cast_value = self._cast_value(value, col_type)

            mask = self._apply_operator(col_data, op, cast_value)
            filtered_data = {
                col: [v for v, m in zip(vals, mask) if m]
                for col, vals in data.items()
            }
            return self._format_result(filtered_data)

        return self._format_result(data)

    def _parse_where(self, clause: str) -> tuple:
        operators = ['>=', '<=', '!=', '=', '>', '<']
        for op in operators:
            if op in clause:
                parts = clause.split(op)
                return parts[0].strip(), op, parts[1].strip()
        raise ValueError("Invalid where clause")

    def _cast_value(self, value: str, col_type: str) -> Any:
        if col_type == 'int':
            return int(value)
        elif col_type == 'float':
            return float(value)
        else:
            return value.strip("'\"")

    def _apply_operator(self, data: List, op: str, value: Any) -> List[bool]:
        ops = {
            '>': lambda a, b: a > b,
            '<': lambda a, b: a < b,
            '>=': lambda a, b: a >= b,
            '<=': lambda a, b: a <= b,
            '=': lambda a, b: a == b,
            '!=': lambda a, b: a != b
        }
        return [ops[op](x, value) for x in data]

    def _format_result(self, data: Dict) -> List[Dict]:
        return [dict(zip(data.keys(), row)) for row in zip(*data.values())]
