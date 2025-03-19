from typing import List, Dict, Any, Tuple, Optional
import re
from .core import ColumnarReader

class QueryEngine:
    def __init__(self, reader: ColumnarReader):
        self.reader = reader
        self.column_cache = {}

    def _get_column(self, col_name: str) -> List[Any]:
        """Get a column from the cache or load it from the reader."""
        if col_name not in self.column_cache:
            self.column_cache[col_name] = self.reader.read_column(col_name)
        return self.column_cache[col_name]

    def execute(self, query: str) -> List[Dict]:
        """
        Execute a SQL-like query on the columnar data.
        Supports SELECT, WHERE, and aggregations (SUM, COUNT, AVG, MIN, MAX).
        """
        # Parse SELECT clause
        select_part, rest = re.split(r'\s+FROM\s+', query, 1, flags=re.IGNORECASE)
        select_columns = [col.strip() for col in select_part.replace('SELECT ', '').split(',')]

        # Parse aggregations and regular columns
        aggregates, regular_cols = self._parse_select(select_columns)

        # Parse WHERE clause
        where_clause = re.split(r'\s+WHERE\s+', rest, 1, flags=re.IGNORECASE)[1].strip() \
            if ' WHERE ' in rest else None

        # Get needed columns
        needed_columns = self._get_needed_columns(regular_cols, aggregates, where_clause)
        data = {col: self._get_column(col) for col in needed_columns}

        # Apply WHERE filtering
        filtered_data = self._apply_where(data, where_clause)

        # Process results
        if aggregates:
            return self._process_aggregates(filtered_data, aggregates)
        return self._format_result(filtered_data)

    def _parse_select(self, select_columns: List[str]) -> Tuple[List[Tuple[str, str]], List[str]]:
        """
        Parse the SELECT clause to identify aggregation functions and regular columns.
        Returns a tuple of (aggregates, regular_columns).
        """
        aggregates = []
        regular_cols = []
        for col in select_columns:
            match = re.match(r'(\w+)\((\w+)\)', col)
            if match:
                aggregates.append((match.group(1).upper(), match.group(2)))
            else:
                regular_cols.append(col)
        return aggregates, regular_cols

    def _get_needed_columns(self, regular_cols: List[str],
                          aggregates: List[Tuple[str, str]],
                          where_clause: Optional[str]) -> List[str]:
        """
        Determine which columns need to be loaded based on the query.
        """
        needed = set(regular_cols)
        needed.update(agg_col for _, agg_col in aggregates)

        if where_clause:
            where_col, _, _ = self._parse_where(where_clause)
            needed.add(where_col)

        return list(needed)

    def _apply_where(self, data: Dict[str, List[Any]], where_clause: Optional[str]) -> Dict[str, List[Any]]:
        """
        Apply the WHERE clause to filter the data.
        """
        if not where_clause:
            return data

        col_name, op, value = self._parse_where(where_clause)
        col_data = data[col_name]
        col_type = next(c['type'] for c in self.reader.metadata['columns'] if c['name'] == col_name)
        cast_value = self._cast_value(value, col_type)

        mask = self._apply_operator(col_data, op, cast_value)
        return {col: [v for v, m in zip(vals, mask) if m] for col, vals in data.items()}

    def _process_aggregates(self, data: Dict[str, List[Any]],
                          aggregates: List[Tuple[str, str]]) -> List[Dict[str, Any]]:
        """
        Calculate aggregate values from the filtered data.
        """
        results = {}
        for func, col in aggregates:
            values = data.get(col, [])
            if func == 'SUM':
                results[f'SUM({col})'] = sum(values)
            elif func == 'COUNT':
                results[f'COUNT({col})'] = len(values)
            elif func == 'AVG':
                results[f'AVG({col})'] = sum(values) / len(values) if values else None
            elif func == 'MIN':
                results[f'MIN({col})'] = min(values) if values else None
            elif func == 'MAX':
                results[f'MAX({col})'] = max(values) if values else None
        return [results]

    def _parse_where(self, clause: str) -> Tuple[str, str, str]:
        """
        Parse the WHERE clause into (column_name, operator, value).
        """
        operators = ['>=', '<=', '!=', '=', '>', '<']
        for op in operators:
            if op in clause:
                left, right = clause.split(op, 1)
                return left.strip(), op, right.strip()
        raise ValueError(f"Invalid where clause: {clause}")

    def _cast_value(self, value: str, col_type: str) -> Any:
        """
        Cast a value to the appropriate type based on the column type.
        """
        if col_type == 'int':
            return int(value)
        elif col_type == 'float':
            return float(value)
        return value.strip("'\"")

    def _apply_operator(self, data: List[Any], op: str, value: Any) -> List[bool]:
        """
        Apply a comparison operator to a list of values.
        """
        ops = {
            '>': lambda a, b: a > b,
            '<': lambda a, b: a < b,
            '>=': lambda a, b: a >= b,
            '<=': lambda a, b: a <= b,
            '=': lambda a, b: a == b,
            '!=': lambda a, b: a != b
        }
        return [ops[op](x, value) for x in data]

    def _format_result(self, data: Dict[str, List[Any]]) -> List[Dict[str, Any]]:
        """
        Format the result as a list of dictionaries.
        """
        return [dict(zip(data.keys(), row)) for row in zip(*data.values())]
