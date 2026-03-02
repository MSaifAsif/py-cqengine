"""
PyCQEngine: High-performance in-memory NoSQL indexing engine for Python collections

A Rust-backed query engine providing sub-100 microsecond latency for point-lookups
on collections of 1,000,000+ objects.
"""

from pycqengine.core import IndexedCollection
from pycqengine.attribute import Attribute
from pycqengine.query import eq, and_, or_, in_, gt, gte, lt, lte, between

__version__ = "0.1.0"
__all__ = [
    "IndexedCollection",
    "Attribute",
    "eq",
    "and_",
    "or_",
    "in_",
    "gt",
    "gte",
    "lt",
    "lte",
    "between",
]
