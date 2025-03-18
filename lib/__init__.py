"""
Simple Columnar Storage System

A lightweight columnar storage implementation with CSV conversion
and basic query capabilities.
"""

from .core import ColumnarWriter, ColumnarReader
from .csv_converter import csv_to_columnar
from .query import QueryEngine

__version__ = "0.1.0"
__all__ = [
    'ColumnarWriter',
    'ColumnarReader',
    'csv_to_columnar',
    'QueryEngine'
]
