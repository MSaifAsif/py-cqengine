"""
Query DSL for building and executing queries
"""

from typing import Any, List, Set, TYPE_CHECKING
from abc import ABC, abstractmethod

if TYPE_CHECKING:
    from pycqengine.core import IndexedCollection
    from pycqengine.attribute import Attribute


class Query(ABC):
    """Base class for all query types"""
    
    @abstractmethod
    def execute(self, collection: 'IndexedCollection') -> List[int]:
        """
        Execute the query against a collection.
        
        Args:
            collection: The collection to query
            
        Returns:
            List of object IDs matching the query
        """
        pass
    
    def quick_count(self, collection: 'IndexedCollection') -> int:
        """
        Fast count without cloning full ID set. Returns None if not supported.
        Override in subclasses for optimized counting.
        """
        return None
    
    def quick_first(self, collection: 'IndexedCollection', n: int) -> list:
        """
        Fast first-N objects without full query execution. Returns None if not supported.
        Override in subclasses for optimized first-N retrieval.
        """
        return None

    def quick_materialize(self, collection: 'IndexedCollection') -> list:
        """
        Fused query + materialization in a single Rust call.
        Avoids the IDs → Python list → back to Rust roundtrip.
        Returns None if not supported.
        """
        return None


class EqualityQuery(Query):
    """Query for exact attribute match"""
    
    def __init__(self, attribute: 'Attribute', value: Any):
        self.attribute = attribute
        self.value = value
    
    def execute(self, collection: 'IndexedCollection') -> List[int]:
        """Execute equality query using hash index"""
        # Rust already returns unique IDs — no set() conversion needed
        return collection._manager.query_eq(self.attribute.name, self.value)
    
    def quick_count(self, collection: 'IndexedCollection') -> int:
        """Count without cloning — O(1) via Rust index"""
        return collection._manager.query_eq_count(self.attribute.name, self.value)
    
    def quick_first(self, collection: 'IndexedCollection', n: int) -> list:
        """Get first N objects without full query — one Rust call"""
        return collection._manager.query_eq_first_objects(self.attribute.name, self.value, n)

    def quick_materialize(self, collection: 'IndexedCollection') -> list:
        """Fused eq query + object materialization (single Rust FFI call)"""
        return collection._manager.query_eq_objects(self.attribute.name, self.value)
    
    def __repr__(self) -> str:
        return f"eq({self.attribute.name}, {self.value!r})"


_SIMPLE_QUERY_TYPES = None  # Populated lazily to avoid circular imports

def _to_query_spec(q):
    """Convert a simple query object to a (op, attr, value...) spec tuple for Rust."""
    # Lazily resolve all supported simple query types
    global _SIMPLE_QUERY_TYPES
    if _SIMPLE_QUERY_TYPES is None:
        _SIMPLE_QUERY_TYPES = {
            'EqualityQuery': lambda q: ("eq", q.attribute.name, q.value),
            'GtQuery':       lambda q: ("gt", q.attribute.name, q.value),
            'GteQuery':      lambda q: ("gte", q.attribute.name, q.value),
            'LtQuery':       lambda q: ("lt", q.attribute.name, q.value),
            'LteQuery':      lambda q: ("lte", q.attribute.name, q.value),
            'BetweenQuery':  lambda q: ("between", q.attribute.name, q.min_val, q.max_val),
        }
    cls_name = type(q).__name__
    converter = _SIMPLE_QUERY_TYPES.get(cls_name)
    if converter is None:
        return None
    return converter(q)


def _all_simple(queries):
    """Try to convert all queries to spec tuples. Returns list or None if any unsupported."""
    specs = []
    for q in queries:
        spec = _to_query_spec(q)
        if spec is None:
            return None
        specs.append(spec)
    return specs


class AndQuery(Query):
    """Query for intersection of multiple sub-queries"""
    
    def __init__(self, *queries: Query):
        if not queries:
            raise ValueError("AND query requires at least one sub-query")
        self.queries = queries
    
    def execute(self, collection: 'IndexedCollection') -> List[int]:
        """Execute AND by intersecting result sets"""
        # Fast path: all EqualityQuery → use optimized Rust query_and
        if all(isinstance(q, EqualityQuery) for q in self.queries):
            query_pairs = [(q.attribute.name, q.value) for q in self.queries]
            return collection._manager.query_and(query_pairs)
        
        # General path: mixed query types → single Rust call
        specs = _all_simple(self.queries)
        if specs is not None:
            return collection._manager.query_and_general(specs)
        
        # Fallback for complex nested queries (uses sets for intersection)
        result = set(self.queries[0].execute(collection))
        
        for query in self.queries[1:]:
            result &= set(query.execute(collection))
            
            if not result:
                break
        
        return list(result)

    def quick_materialize(self, collection: 'IndexedCollection') -> list:
        """Fused AND query + object materialization (single Rust FFI call)"""
        if all(isinstance(q, EqualityQuery) for q in self.queries):
            query_pairs = [(q.attribute.name, q.value) for q in self.queries]
            return collection._manager.query_and_objects(query_pairs)
        # General fused path for mixed queries
        specs = _all_simple(self.queries)
        if specs is not None:
            return collection._manager.query_and_general_objects(specs)
        return None
    
    def __repr__(self) -> str:
        return f"and_({', '.join(repr(q) for q in self.queries)})"


class OrQuery(Query):
    """Query for union of multiple sub-queries"""
    
    def __init__(self, *queries: Query):
        if not queries:
            raise ValueError("OR query requires at least one sub-query")
        self.queries = queries
    
    def execute(self, collection: 'IndexedCollection') -> List[int]:
        """Execute OR by unioning result sets"""
        # Optimization: If all queries are EqualityQuery, use Rust query_or
        if all(isinstance(q, EqualityQuery) for q in self.queries):
            query_pairs = [(q.attribute.name, q.value) for q in self.queries]
            return collection._manager.query_or(query_pairs)
        
        # Fallback for complex nested queries (uses sets for union)
        result: Set[int] = set()
        
        for query in self.queries:
            result |= set(query.execute(collection))
        
        return list(result)

    def quick_materialize(self, collection: 'IndexedCollection') -> list:
        """Fused OR query + object materialization (single Rust FFI call)"""
        if all(isinstance(q, EqualityQuery) for q in self.queries):
            query_pairs = [(q.attribute.name, q.value) for q in self.queries]
            return collection._manager.query_or_objects(query_pairs)
        return None
    
    def __repr__(self) -> str:
        return f"or_({', '.join(repr(q) for q in self.queries)})"


class InQuery(Query):
    """Query for attribute value in a set of values"""
    
    def __init__(self, attribute: 'Attribute', values: Any):
        self.attribute = attribute
        # Convert to list for Rust (preserve order but still efficient)
        self.values = list(values) if not isinstance(values, (list, set)) else list(values) if isinstance(values, set) else values
    
    def execute(self, collection: 'IndexedCollection') -> Set[int]:
        """Execute IN query using optimized Rust query_in"""
        return collection._manager.query_in(self.attribute.name, self.values)

    def quick_materialize(self, collection: 'IndexedCollection') -> list:
        """Fused IN query + object materialization (single Rust FFI call)"""
        return collection._manager.query_in_objects(self.attribute.name, self.values)
    
    def __repr__(self) -> str:
        return f"in_({self.attribute.name}, {self.values!r})"


# Query DSL factory functions

def eq(attribute: 'Attribute', value: Any) -> EqualityQuery:
    """
    Create an equality query.
    
    Example:
        >>> results = cars.retrieve(eq(BRAND, "Tesla"))
    
    Args:
        attribute: Attribute to match
        value: Value to match against
        
    Returns:
        EqualityQuery instance
    """
    return EqualityQuery(attribute, value)


def and_(*queries: Query) -> AndQuery:
    """
    Create an AND query (intersection).
    
    Example:
        >>> results = cars.retrieve(and_(
        ...     eq(BRAND, "Tesla"),
        ...     eq(COLOR, "Red")
        ... ))
    
    Args:
        *queries: Sub-queries to intersect
        
    Returns:
        AndQuery instance
    """
    return AndQuery(*queries)


def or_(*queries: Query) -> OrQuery:
    """
    Create an OR query (union).
    
    Example:
        >>> results = cars.retrieve(or_(
        ...     eq(BRAND, "Tesla"),
        ...     eq(BRAND, "Ford")
        ... ))
    
    Args:
        *queries: Sub-queries to union
        
    Returns:
        OrQuery instance
    """
    return OrQuery(*queries)


def in_(attribute: 'Attribute', values: Any) -> InQuery:
    """
    Create an IN query (value membership).
    
    Example:
        >>> results = cars.retrieve(in_(BRAND, ["Tesla", "Ford", "BMW"]))
    
    Args:
        attribute: Attribute to check
        values: Iterable of values to match against
        
    Returns:
        InQuery instance
    """
    return InQuery(attribute, values)


# ── Range query classes ──────────────────────────────────────────────

class GtQuery(Query):
    """Query for attribute > value (requires btree index)"""

    def __init__(self, attribute: 'Attribute', value: Any):
        self.attribute = attribute
        self.value = value

    def execute(self, collection: 'IndexedCollection') -> List[int]:
        return collection._manager.query_gt(self.attribute.name, self.value)

    def quick_count(self, collection: 'IndexedCollection') -> int:
        return collection._manager.query_gt_count(self.attribute.name, self.value)

    def quick_materialize(self, collection: 'IndexedCollection') -> list:
        return collection._manager.query_gt_objects(self.attribute.name, self.value)

    def __repr__(self) -> str:
        return f"gt({self.attribute.name}, {self.value!r})"


class GteQuery(Query):
    """Query for attribute >= value (requires btree index)"""

    def __init__(self, attribute: 'Attribute', value: Any):
        self.attribute = attribute
        self.value = value

    def execute(self, collection: 'IndexedCollection') -> List[int]:
        return collection._manager.query_gte(self.attribute.name, self.value)

    def quick_materialize(self, collection: 'IndexedCollection') -> list:
        return collection._manager.query_gte_objects(self.attribute.name, self.value)

    def __repr__(self) -> str:
        return f"gte({self.attribute.name}, {self.value!r})"


class LtQuery(Query):
    """Query for attribute < value (requires btree index)"""

    def __init__(self, attribute: 'Attribute', value: Any):
        self.attribute = attribute
        self.value = value

    def execute(self, collection: 'IndexedCollection') -> List[int]:
        return collection._manager.query_lt(self.attribute.name, self.value)

    def quick_materialize(self, collection: 'IndexedCollection') -> list:
        return collection._manager.query_lt_objects(self.attribute.name, self.value)

    def __repr__(self) -> str:
        return f"lt({self.attribute.name}, {self.value!r})"


class LteQuery(Query):
    """Query for attribute <= value (requires btree index)"""

    def __init__(self, attribute: 'Attribute', value: Any):
        self.attribute = attribute
        self.value = value

    def execute(self, collection: 'IndexedCollection') -> List[int]:
        return collection._manager.query_lte(self.attribute.name, self.value)

    def quick_materialize(self, collection: 'IndexedCollection') -> list:
        return collection._manager.query_lte_objects(self.attribute.name, self.value)

    def __repr__(self) -> str:
        return f"lte({self.attribute.name}, {self.value!r})"


class BetweenQuery(Query):
    """Query for min_val <= attribute <= max_val (requires btree index)"""

    def __init__(self, attribute: 'Attribute', min_val: Any, max_val: Any):
        self.attribute = attribute
        self.min_val = min_val
        self.max_val = max_val

    def execute(self, collection: 'IndexedCollection') -> List[int]:
        return collection._manager.query_between(
            self.attribute.name, self.min_val, self.max_val
        )

    def quick_count(self, collection: 'IndexedCollection') -> int:
        return collection._manager.query_between_count(
            self.attribute.name, self.min_val, self.max_val
        )

    def quick_materialize(self, collection: 'IndexedCollection') -> list:
        return collection._manager.query_between_objects(
            self.attribute.name, self.min_val, self.max_val
        )

    def __repr__(self) -> str:
        return f"between({self.attribute.name}, {self.min_val!r}, {self.max_val!r})"


# ── Range query factory functions ────────────────────────────────────

def gt(attribute: 'Attribute', value: Any) -> GtQuery:
    """
    Create a greater-than query.  Requires a btree index on the attribute.

    Example:
        >>> results = cars.retrieve(gt(PRICE, 30000))
    """
    return GtQuery(attribute, value)


def gte(attribute: 'Attribute', value: Any) -> GteQuery:
    """
    Create a greater-than-or-equal query.  Requires a btree index.

    Example:
        >>> results = cars.retrieve(gte(PRICE, 30000))
    """
    return GteQuery(attribute, value)


def lt(attribute: 'Attribute', value: Any) -> LtQuery:
    """
    Create a less-than query.  Requires a btree index.

    Example:
        >>> results = cars.retrieve(lt(PRICE, 50000))
    """
    return LtQuery(attribute, value)


def lte(attribute: 'Attribute', value: Any) -> LteQuery:
    """
    Create a less-than-or-equal query.  Requires a btree index.

    Example:
        >>> results = cars.retrieve(lte(PRICE, 50000))
    """
    return LteQuery(attribute, value)


def between(attribute: 'Attribute', min_val: Any, max_val: Any) -> BetweenQuery:
    """
    Create a between query (inclusive on both ends).  Requires a btree index.

    Example:
        >>> results = cars.retrieve(between(PRICE, 20000, 50000))
    """
    return BetweenQuery(attribute, min_val, max_val)
