"""
Core IndexedCollection implementation
"""

from typing import Any, Iterable, Iterator, List, Optional, Union
from pycqengine._rs import CollectionManager
from pycqengine.attribute import Attribute
from pycqengine.query import Query


class ResultSet:
    """
    Lazy result set over query results.
    
    Delays both query execution AND materialization until actually needed.
    For simple equality queries, uses optimized Rust paths that avoid
    cloning entire ID sets.
    
    Supports count(), first(), slicing, and full iteration.
    Caches materialized objects to avoid repeated FFI calls.
    """
    
    __slots__ = ('_query', '_collection', '_manager', '_object_ids', '_cached_objects', '_count')
    
    def __init__(self, query: 'Query', collection: 'IndexedCollection'):
        self._query = query
        self._collection = collection
        self._manager = collection._manager
        self._object_ids = None   # Lazy: query not yet executed
        self._cached_objects = None
        self._count = None
    
    def _ensure_ids(self) -> List[int]:
        """Execute the query to get IDs (deferred until needed)"""
        if self._object_ids is None:
            self._object_ids = self._query.execute(self._collection)
            self._count = len(self._object_ids)
        return self._object_ids
    
    def _materialize(self) -> List[Any]:
        """Materialize all objects in a single batch (cached).
        
        Tries fused query+materialize first (avoids IDs roundtrip),
        falls back to standard IDs → get_objects path.
        """
        if self._cached_objects is None:
            # Fast path: fused query + materialize in single Rust call
            fused = self._query.quick_materialize(self._collection)
            if fused is not None:
                self._cached_objects = fused
                self._count = len(fused)
            else:
                self._cached_objects = self._manager.get_objects(self._ensure_ids())
        return self._cached_objects
    
    def count(self) -> int:
        """
        Return number of results WITHOUT materializing objects.
        
        For equality queries, this is zero-allocation (no ID clone).
        """
        if self._count is not None:
            return self._count
        # Try fast path (no full query execution needed)
        fast = self._query.quick_count(self._collection)
        if fast is not None:
            self._count = fast
            return self._count
        # Fallback: execute full query to get IDs, then count
        self._ensure_ids()
        return self._count
    
    def first(self, n: int = 1) -> List[Any]:
        """
        Return the first N results, materializing only those objects.
        
        For equality queries, this is a single Rust call — no full
        ID set clone, no round-trip with all IDs.
        
        Args:
            n: Number of results to return (default: 1)
            
        Returns:
            List of up to N objects
        """
        if n <= 0:
            return []
        if self._cached_objects is not None:
            return self._cached_objects[:n]
        # Try fast path (bypasses full query execution)
        fast = self._query.quick_first(self._collection, n)
        if fast is not None:
            return fast
        # Fallback: execute query then slice
        return self._manager.get_objects_slice(self._ensure_ids(), 0, n)
    
    def slice(self, start: int, end: int) -> List[Any]:
        """
        Return a slice of results, materializing only those objects.
        
        Useful for pagination: results.slice(0, 50) for page 1,
        results.slice(50, 100) for page 2, etc.
        
        Args:
            start: Start index (inclusive)
            end: End index (exclusive)
            
        Returns:
            List of objects in the [start, end) range
        """
        if self._cached_objects is not None:
            return self._cached_objects[start:end]
        return self._manager.get_objects_slice(self._ensure_ids(), start, end)
    
    def __iter__(self) -> Iterator[Any]:
        """Return iterator over all results (materializes all objects)"""
        return iter(self._materialize())
    
    def __len__(self) -> int:
        """Return number of results WITHOUT materializing"""
        return self.count()
    
    def __bool__(self) -> bool:
        """Return True if there are any results (no materialization)"""
        return self.count() > 0
    
    def __getitem__(self, key: Union[int, slice]) -> Any:
        """
        Support indexing and slicing.
        
        results[0] → first object (materializes 1 object)
        results[0:10] → first 10 objects (materializes 10 objects)
        """
        if isinstance(key, slice):
            start, stop, step = key.indices(self.count())
            if step != 1:
                # Complex slicing — materialize all then slice
                return self._materialize()[key]
            return self.slice(start, stop)
        elif isinstance(key, int):
            total = self.count()
            if key < 0:
                key = total + key
            if key < 0 or key >= total:
                raise IndexError(f"ResultSet index {key} out of range")
            if self._cached_objects is not None:
                return self._cached_objects[key]
            # For single item, use first() fast path if available
            result = self.first(key + 1)
            if key < len(result):
                return result[key]
            return None
        else:
            raise TypeError(f"ResultSet indices must be integers or slices, not {type(key).__name__}")
    
    def __contains__(self, obj: Any) -> bool:
        """
        Check if an object is in the result set.
        
        Uses internal slot ID comparison — requires query execution but no materialization.
        """
        slot = self._manager.object_slot(obj)
        if slot is None:
            return False
        return slot in self._ensure_ids()
    
    def __repr__(self) -> str:
        return f"ResultSet(size={self.count()})"


class IndexedCollection:
    """
    High-performance indexed collection for Python objects.
    
    This class provides a NoSQL-like query interface with automatic indexing,
    backed by Rust for sub-100 microsecond query latencies.
    
    Example:
        >>> from pycqengine import IndexedCollection, Attribute, eq
        >>> 
        >>> class Car:
        ...     def __init__(self, vin, brand, price):
        ...         self.vin = vin
        ...         self.brand = brand
        ...         self.price = price
        ...
        >>> VIN = Attribute("vin", lambda c: c.vin)
        >>> BRAND = Attribute("brand", lambda c: c.brand)
        >>>
        >>> cars = IndexedCollection()
        >>> cars.add_index(VIN)
        >>> cars.add_index(BRAND)
        >>>
        >>> cars.add(Car(1, "Tesla", 50000))
        >>> cars.add(Car(2, "Ford", 30000))
        >>>
        >>> results = cars.retrieve(eq(BRAND, "Tesla"))
        >>> for car in results:
        ...     print(car.vin, car.brand)
        1 Tesla
    """
    
    def __init__(self, use_weakrefs: bool = False):
        """Initialize an empty indexed collection.
        
        Args:
            use_weakrefs: If True, store weak references to objects instead of
                strong references. Objects that are no longer referenced elsewhere
                will be automatically cleaned up during gc() or lazily during
                queries. Objects that don't support weakrefs (e.g. built-in types)
                fall back to strong references automatically.
        """
        self._manager = CollectionManager(use_weakrefs)
        self._indexes = {}  # attribute name -> Attribute
        self._use_weakrefs = use_weakrefs
    
    def add_index(self, attribute: Attribute, index_type: str = "hash") -> None:
        """
        Add an index for an attribute.
        
        Must be called before adding objects to the collection for the
        attribute to be indexed.
        
        Args:
            attribute: Attribute to index
            index_type: "hash" (default, O(1) equality) or "btree" (range queries: gt/lt/gte/lte/between)
        """
        self._manager.add_index(attribute.name, index_type)
        self._indexes[attribute.name] = attribute
    
    def add(self, obj: Any) -> None:
        """
        Add a single object to the collection.
        
        Extracts indexed attributes and updates all indexes.
        
        Args:
            obj: Object to add
        """
        # Extract attribute values using registered lambdas
        attributes = {}
        for attr_name, attribute in self._indexes.items():
            try:
                value = attribute.extract(obj)
                attributes[attr_name] = value
            except (AttributeError, KeyError):
                # Skip missing attributes
                pass
        
        # Add to Rust collection manager
        self._manager.add_object(obj, attributes)
    
    def add_many(self, objects: Iterable[Any]) -> None:
        """
        Add multiple objects in batch.
        
        More efficient than calling add() repeatedly due to reduced
        Python-Rust FFI overhead.
        
        Args:
            objects: Iterable of objects to add
        """
        batch = []
        
        for obj in objects:
            # Extract attributes
            attributes = {}
            for attr_name, attribute in self._indexes.items():
                try:
                    value = attribute.extract(obj)
                    attributes[attr_name] = value
                except (AttributeError, KeyError):
                    pass
            
            batch.append((obj, attributes))
        
        # Send batch to Rust
        self._manager.add_objects_batch(batch)
    
    def retrieve(self, query: Query) -> 'ResultSet':
        """
        Execute a query and return results.
        
        Returns a fully lazy ResultSet that delays BOTH query execution
        and object materialization until needed.
        
        For equality queries, count() and first() use optimized Rust
        paths that avoid cloning the full ID set.
        
        Args:
            query: Query to execute (created using eq, and_, or_, etc.)
            
        Returns:
            ResultSet with lazy query execution and materialization
        """
        # Return lazy result set — query not executed yet!
        return ResultSet(query, self)
    
    def __len__(self) -> int:
        """Return number of objects in the collection"""
        return self._manager.size()
    
    def __del__(self) -> None:
        """Release all object references on garbage collection"""
        try:
            self._manager.clear()
        except Exception:
            pass
    
    def remove(self, obj: Any) -> bool:
        """
        Remove a single object from the collection and all indexes.
        
        Args:
            obj: Object to remove (must be the same object instance that was added)
            
        Returns:
            True if the object was found and removed, False otherwise
        """
        attributes = {}
        for attr_name, attribute in self._indexes.items():
            try:
                value = attribute.extract(obj)
                attributes[attr_name] = value
            except (AttributeError, KeyError):
                pass
        
        return self._manager.remove_object(obj, attributes)
    
    def remove_many(self, objects: Iterable[Any]) -> int:
        """
        Remove multiple objects in batch with parallel index cleanup.
        
        More efficient than calling remove() repeatedly.
        
        Args:
            objects: Iterable of objects to remove
            
        Returns:
            Number of objects actually removed
        """
        batch = []
        for obj in objects:
            attributes = {}
            for attr_name, attribute in self._indexes.items():
                try:
                    value = attribute.extract(obj)
                    attributes[attr_name] = value
                except (AttributeError, KeyError):
                    pass
            batch.append((obj, attributes))
        
        return self._manager.remove_objects_batch(batch)
    
    def clear(self) -> None:
        """Remove all objects and clear all indexes, freeing memory"""
        self._manager.clear()

    def gc(self) -> int:
        """Garbage-collect dead weak references and clean their index entries.
        
        Only meaningful when use_weakrefs=True. In strong-ref mode this is a no-op
        returning 0.
        
        Returns:
            Number of dead references cleaned up
        """
        return self._manager.gc()

    @property
    def alive_count(self) -> int:
        """Number of objects still alive in the collection.
        
        In strong-ref mode this equals len(). In weak-ref mode, this checks
        each stored weak reference and counts only the still-alive ones.
        """
        return self._manager.alive_count()

    @property
    def use_weakrefs(self) -> bool:
        """Whether this collection stores weak references."""
        return self._use_weakrefs
