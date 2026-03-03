"""
Tests for PyWeakref / self-cleaning registry feature.

Covers:
- Opt-in weakref mode via use_weakrefs=True
- Objects surviving when Python references are held
- Objects becoming unreachable after Python references are dropped
- gc() cleaning dead weakrefs and their index entries
- alive_count tracking
- Lazy cleanup during queries
- Fallback to strong refs for objects that don't support weakrefs
- Address reuse handling
- Batch add/remove with weakrefs
"""

import gc as python_gc
import pytest
from pycqengine import IndexedCollection, Attribute, eq, and_, or_, in_


class Car:
    """Test object (supports weakrefs by default in Python)."""
    def __init__(self, vin, brand, price):
        self.vin = vin
        self.brand = brand
        self.price = price

    def __repr__(self):
        return f"Car(vin={self.vin}, brand={self.brand!r}, price={self.price})"


VIN = Attribute("vin", lambda c: c.vin)
BRAND = Attribute("brand", lambda c: c.brand)
PRICE = Attribute("price", lambda c: c.price)


def make_collection(use_weakrefs=True, indexes=None):
    """Helper to create an indexed collection with standard indexes."""
    col = IndexedCollection(use_weakrefs=use_weakrefs)
    for idx in (indexes or [VIN, BRAND, PRICE]):
        col.add_index(idx)
    return col


# ---------- basic weakref mode ----------

class TestWeakrefBasic:

    def test_use_weakrefs_property_true(self):
        col = IndexedCollection(use_weakrefs=True)
        assert col.use_weakrefs is True

    def test_use_weakrefs_property_false(self):
        col = IndexedCollection(use_weakrefs=False)
        assert col.use_weakrefs is False

    def test_default_is_strong_ref(self):
        col = IndexedCollection()
        assert col.use_weakrefs is False

    def test_add_and_retrieve_weakref_mode(self):
        col = make_collection(use_weakrefs=True)
        car = Car(1, "Tesla", 50000)
        col.add(car)
        results = list(col.retrieve(eq(BRAND, "Tesla")))
        assert len(results) == 1
        assert results[0].vin == 1

    def test_add_many_weakref_mode(self):
        col = make_collection(use_weakrefs=True)
        cars = [Car(i, "Brand", i * 1000) for i in range(10)]
        col.add_many(cars)
        assert len(col) == 10
        results = list(col.retrieve(eq(BRAND, "Brand")))
        assert len(results) == 10


# ---------- object lifecycle ----------

class TestWeakrefLifecycle:

    def test_objects_survive_with_reference(self):
        """Objects should be retrievable as long as Python refs are held."""
        col = make_collection(use_weakrefs=True)
        cars = [Car(1, "Tesla", 50000), Car(2, "Ford", 30000)]
        col.add_many(cars)

        assert col.alive_count == 2
        results = list(col.retrieve(eq(BRAND, "Tesla")))
        assert len(results) == 1
        assert results[0] is cars[0]

    def test_objects_disappear_after_del_and_gc(self):
        """After dropping all Python refs and running gc, objects should be gone."""
        col = make_collection(use_weakrefs=True)
        car = Car(1, "Tesla", 50000)
        col.add(car)
        assert col.alive_count == 1

        # Drop the only Python reference
        del car
        python_gc.collect()

        # alive_count should detect the dead ref
        assert col.alive_count == 0

        # gc should clean the stale entry
        cleaned = col.gc()
        assert cleaned == 1

        # Collection should now be effectively empty
        results = list(col.retrieve(eq(BRAND, "Tesla")))
        assert len(results) == 0

    def test_partial_gc(self):
        """Only dead refs should be cleaned; live ones stay."""
        col = make_collection(use_weakrefs=True)
        keep = Car(1, "Tesla", 50000)
        lose = Car(2, "Ford", 30000)
        col.add(keep)
        col.add(lose)
        assert col.alive_count == 2

        del lose
        python_gc.collect()

        assert col.alive_count == 1
        cleaned = col.gc()
        assert cleaned == 1

        # The kept car should still be queryable
        results = list(col.retrieve(eq(BRAND, "Tesla")))
        assert len(results) == 1
        assert results[0] is keep

        # The lost car should be gone
        results = list(col.retrieve(eq(BRAND, "Ford")))
        assert len(results) == 0

    def test_gc_noop_in_strong_mode(self):
        """gc() should return 0 in strong-ref mode."""
        col = make_collection(use_weakrefs=False)
        car = Car(1, "Tesla", 50000)
        col.add(car)
        del car
        python_gc.collect()
        assert col.gc() == 0
        # Strong ref keeps object alive
        assert len(col) == 1


# ---------- lazy cleanup during queries ----------

class TestWeakrefLazyCleanup:

    def test_dead_refs_cleaned_during_query(self):
        """Queries should skip dead refs and clean them lazily."""
        col = make_collection(use_weakrefs=True)
        keep = Car(1, "Tesla", 50000)
        col.add(keep)
        lose = Car(2, "Tesla", 60000)
        col.add(lose)

        del lose
        python_gc.collect()

        # Query should only return the live object
        results = list(col.retrieve(eq(BRAND, "Tesla")))
        assert len(results) == 1
        assert results[0] is keep

    def test_and_query_with_dead_refs(self):
        """AND queries should handle dead refs properly."""
        col = make_collection(use_weakrefs=True)
        keep = Car(1, "Tesla", 50000)
        lose = Car(2, "Tesla", 50000)
        col.add(keep)
        col.add(lose)

        del lose
        python_gc.collect()

        results = list(col.retrieve(and_(eq(BRAND, "Tesla"), eq(PRICE, 50000))))
        assert len(results) == 1
        assert results[0] is keep

    def test_or_query_with_dead_refs(self):
        """OR queries should handle dead refs properly."""
        col = make_collection(use_weakrefs=True)
        keep = Car(1, "Tesla", 50000)
        lose = Car(2, "Ford", 30000)
        col.add(keep)
        col.add(lose)

        del lose
        python_gc.collect()

        results = list(col.retrieve(or_(eq(BRAND, "Tesla"), eq(BRAND, "Ford"))))
        assert len(results) == 1
        assert results[0] is keep

    def test_in_query_with_dead_refs(self):
        """IN queries should handle dead refs properly."""
        col = make_collection(use_weakrefs=True)
        keep = Car(1, "Tesla", 50000)
        lose = Car(2, "Ford", 30000)
        col.add(keep)
        col.add(lose)

        del lose
        python_gc.collect()

        results = list(col.retrieve(in_(BRAND, ["Tesla", "Ford"])))
        assert len(results) == 1
        assert results[0] is keep


# ---------- remove with weakrefs ----------

class TestWeakrefRemove:

    def test_remove_in_weakref_mode(self):
        col = make_collection(use_weakrefs=True)
        car = Car(1, "Tesla", 50000)
        col.add(car)
        assert col.alive_count == 1

        col.remove(car)
        assert col.alive_count == 0
        results = list(col.retrieve(eq(BRAND, "Tesla")))
        assert len(results) == 0

    def test_remove_many_in_weakref_mode(self):
        col = make_collection(use_weakrefs=True)
        cars = [Car(i, "Tesla", i * 1000) for i in range(5)]
        col.add_many(cars)
        assert col.alive_count == 5

        removed = col.remove_many(cars[:3])
        assert removed == 3
        assert col.alive_count == 2


# ---------- clear ----------

class TestWeakrefClear:

    def test_clear_resets_everything(self):
        col = make_collection(use_weakrefs=True)
        cars = [Car(i, "Tesla", i * 1000) for i in range(5)]
        col.add_many(cars)
        assert col.alive_count == 5

        col.clear()
        assert len(col) == 0
        assert col.alive_count == 0


# ---------- mixed strong/weak (fallback) ----------

class TestWeakrefFallback:

    def test_tuple_objects_fallback_to_strong(self):
        """Tuples don't support weakrefs — should fall back to strong refs."""
        col = IndexedCollection(use_weakrefs=True)
        NAME = Attribute("name", lambda t: t[0])
        col.add_index(NAME)

        obj = ("hello", 42)
        col.add(obj)
        
        # Should be retrievable
        results = list(col.retrieve(eq(NAME, "hello")))
        assert len(results) == 1
        assert results[0] == ("hello", 42)

        # Even after deleting our ref, still alive (strong ref fallback)
        del obj
        python_gc.collect()
        assert col.alive_count == 1
        results = list(col.retrieve(eq(NAME, "hello")))
        assert len(results) == 1


# ---------- alive_count ----------

class TestAliveCount:

    def test_alive_count_strong_mode(self):
        col = make_collection(use_weakrefs=False)
        col.add(Car(1, "Tesla", 50000))
        col.add(Car(2, "Ford", 30000))
        assert col.alive_count == 2

    def test_alive_count_weakref_mode_all_alive(self):
        col = make_collection(use_weakrefs=True)
        cars = [Car(1, "Tesla", 50000), Car(2, "Ford", 30000)]
        col.add_many(cars)
        assert col.alive_count == 2

    def test_alive_count_after_partial_death(self):
        col = make_collection(use_weakrefs=True)
        keep1 = Car(1, "Tesla", 50000)
        keep2 = Car(2, "Ford", 30000)
        lose = Car(3, "BMW", 45000)
        col.add_many([keep1, keep2, lose])
        assert col.alive_count == 3

        del lose
        python_gc.collect()
        assert col.alive_count == 2


# ---------- gc details ----------

class TestGarbageCollection:

    def test_gc_returns_zero_when_all_alive(self):
        col = make_collection(use_weakrefs=True)
        cars = [Car(1, "Tesla", 50000)]
        col.add_many(cars)
        assert col.gc() == 0

    def test_gc_cleans_multiple_dead(self):
        col = make_collection(use_weakrefs=True)
        lose1 = Car(1, "Tesla", 50000)
        lose2 = Car(2, "Ford", 30000)
        lose3 = Car(3, "BMW", 45000)
        col.add_many([lose1, lose2, lose3])

        del lose1, lose2, lose3
        python_gc.collect()

        cleaned = col.gc()
        assert cleaned == 3
        assert col.alive_count == 0
        assert len(list(col.retrieve(eq(BRAND, "Tesla")))) == 0

    def test_gc_idempotent(self):
        """Running gc twice should clean nothing the second time."""
        col = make_collection(use_weakrefs=True)
        lose = Car(1, "Tesla", 50000)
        col.add(lose)
        del lose
        python_gc.collect()

        assert col.gc() == 1
        assert col.gc() == 0

    def test_gc_cleans_index_entries(self):
        """After gc, index lookups should NOT find dead object IDs."""
        col = make_collection(use_weakrefs=True)
        lose = Car(1, "Tesla", 50000)
        keep = Car(2, "Ford", 30000)
        col.add(lose)
        col.add(keep)

        del lose
        python_gc.collect()
        col.gc()

        # eq query for Tesla should return nothing
        results = list(col.retrieve(eq(BRAND, "Tesla")))
        assert len(results) == 0

        # eq query for Ford should still return the kept car
        results = list(col.retrieve(eq(BRAND, "Ford")))
        assert len(results) == 1
        assert results[0] is keep
