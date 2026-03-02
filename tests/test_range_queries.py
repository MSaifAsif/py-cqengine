"""
Tests for BTree range-query support: gt, gte, lt, lte, between
"""

import pytest
from pycqengine import (
    IndexedCollection, Attribute,
    eq, and_, or_, gt, gte, lt, lte, between,
)


# ── Helper fixtures ──────────────────────────────────────────────────

class Product:
    def __init__(self, name, price, rating):
        self.name = name
        self.price = price
        self.rating = rating

    def __repr__(self):
        return f"Product({self.name!r}, price={self.price}, rating={self.rating})"


NAME = Attribute("name", lambda p: p.name)
PRICE = Attribute("price", lambda p: p.price)
RATING = Attribute("rating", lambda p: p.rating)


@pytest.fixture
def products():
    """Collection with 5 products, PRICE indexed as btree, NAME as hash."""
    col = IndexedCollection()
    col.add_index(NAME)                    # hash (default)
    col.add_index(PRICE, index_type="btree")  # btree for range queries
    col.add_index(RATING, index_type="btree")

    items = [
        Product("Widget",  10, 4.5),
        Product("Gadget",  25, 3.0),
        Product("Gizmo",   50, 4.0),
        Product("Doohick", 75, 2.5),
        Product("Thingam", 100, 5.0),
    ]
    col.add_many(items)
    return col, items


# ── gt ───────────────────────────────────────────────────────────────

class TestGtQuery:
    def test_gt_basic(self, products):
        col, items = products
        rs = col.retrieve(gt(PRICE, 50))
        names = {p.name for p in rs}
        assert names == {"Doohick", "Thingam"}

    def test_gt_boundary_exclusive(self, products):
        col, _ = products
        rs = col.retrieve(gt(PRICE, 100))
        assert len(rs) == 0

    def test_gt_below_min(self, products):
        col, _ = products
        rs = col.retrieve(gt(PRICE, 5))
        assert len(rs) == 5

    def test_gt_count(self, products):
        col, _ = products
        rs = col.retrieve(gt(PRICE, 25))
        assert rs.count() == 3  # 50, 75, 100

    def test_gt_repr(self):
        q = gt(PRICE, 42)
        assert "gt" in repr(q)


# ── gte ──────────────────────────────────────────────────────────────

class TestGteQuery:
    def test_gte_includes_boundary(self, products):
        col, _ = products
        rs = col.retrieve(gte(PRICE, 50))
        names = {p.name for p in rs}
        assert names == {"Gizmo", "Doohick", "Thingam"}

    def test_gte_all(self, products):
        col, _ = products
        rs = col.retrieve(gte(PRICE, 10))
        assert len(rs) == 5

    def test_gte_none(self, products):
        col, _ = products
        rs = col.retrieve(gte(PRICE, 999))
        assert len(rs) == 0


# ── lt ───────────────────────────────────────────────────────────────

class TestLtQuery:
    def test_lt_basic(self, products):
        col, _ = products
        rs = col.retrieve(lt(PRICE, 50))
        names = {p.name for p in rs}
        assert names == {"Widget", "Gadget"}

    def test_lt_boundary_exclusive(self, products):
        col, _ = products
        rs = col.retrieve(lt(PRICE, 10))
        assert len(rs) == 0

    def test_lt_above_max(self, products):
        col, _ = products
        rs = col.retrieve(lt(PRICE, 200))
        assert len(rs) == 5


# ── lte ──────────────────────────────────────────────────────────────

class TestLteQuery:
    def test_lte_includes_boundary(self, products):
        col, _ = products
        rs = col.retrieve(lte(PRICE, 50))
        names = {p.name for p in rs}
        assert names == {"Widget", "Gadget", "Gizmo"}

    def test_lte_all(self, products):
        col, _ = products
        rs = col.retrieve(lte(PRICE, 100))
        assert len(rs) == 5


# ── between ──────────────────────────────────────────────────────────

class TestBetweenQuery:
    def test_between_inclusive(self, products):
        col, _ = products
        rs = col.retrieve(between(PRICE, 25, 75))
        names = {p.name for p in rs}
        assert names == {"Gadget", "Gizmo", "Doohick"}

    def test_between_single_match(self, products):
        col, _ = products
        rs = col.retrieve(between(PRICE, 50, 50))
        assert len(rs) == 1
        assert list(rs)[0].name == "Gizmo"

    def test_between_no_match(self, products):
        col, _ = products
        rs = col.retrieve(between(PRICE, 200, 300))
        assert len(rs) == 0

    def test_between_count(self, products):
        col, _ = products
        rs = col.retrieve(between(PRICE, 10, 50))
        assert rs.count() == 3  # 10, 25, 50

    def test_between_repr(self):
        q = between(PRICE, 10, 50)
        assert "between" in repr(q)


# ── Composability (range + equality) ────────────────────────────────

class TestRangeComposition:
    def test_and_range_and_eq(self, products):
        """gt(PRICE, 20) AND eq(NAME, 'Gizmo') → just Gizmo (price 50)"""
        col, _ = products
        rs = col.retrieve(and_(gt(PRICE, 20), eq(NAME, "Gizmo")))
        assert len(rs) == 1
        assert list(rs)[0].name == "Gizmo"

    def test_and_range_and_eq_no_match(self, products):
        """gt(PRICE, 60) AND eq(NAME, 'Gadget') → empty (Gadget is 25)"""
        col, _ = products
        rs = col.retrieve(and_(gt(PRICE, 60), eq(NAME, "Gadget")))
        assert len(rs) == 0

    def test_or_range_queries(self, products):
        """lt(PRICE, 20) OR gt(PRICE, 80) → Widget(10), Thingam(100)"""
        col, _ = products
        rs = col.retrieve(or_(lt(PRICE, 20), gt(PRICE, 80)))
        names = {p.name for p in rs}
        assert names == {"Widget", "Thingam"}

    def test_and_two_ranges_same_attr(self, products):
        """gte(PRICE, 25) AND lte(PRICE, 75) → same as between(25, 75)"""
        col, _ = products
        rs = col.retrieve(and_(gte(PRICE, 25), lte(PRICE, 75)))
        names = {p.name for p in rs}
        assert names == {"Gadget", "Gizmo", "Doohick"}

    def test_and_ranges_different_attrs(self, products):
        """gte(PRICE, 50) AND gte(RATING, 4.0) → Gizmo(50,4.0), Thingam(100,5.0)"""
        col, _ = products
        rs = col.retrieve(and_(gte(PRICE, 50), gte(RATING, 4.0)))
        names = {p.name for p in rs}
        assert names == {"Gizmo", "Thingam"}


# ── Edge cases & error handling ──────────────────────────────────────

class TestRangeEdgeCases:
    def test_hash_index_rejects_range_query(self):
        """Range query on a hash-indexed attribute should raise TypeError."""
        col = IndexedCollection()
        col.add_index(NAME)  # hash index
        col.add_many([Product("A", 1, 1.0)])
        with pytest.raises(TypeError):
            col.retrieve(gt(NAME, "A")).count()

    def test_btree_index_supports_equality(self, products):
        """BTree index still supports eq() queries."""
        col, _ = products
        rs = col.retrieve(eq(PRICE, 50))
        assert len(rs) == 1
        assert list(rs)[0].name == "Gizmo"

    def test_btree_index_supports_in(self, products):
        """BTree index still works with in_() queries."""
        from pycqengine import in_
        col, _ = products
        rs = col.retrieve(in_(PRICE, [10, 100]))
        names = {p.name for p in rs}
        assert names == {"Widget", "Thingam"}

    def test_empty_collection_range(self):
        col = IndexedCollection()
        col.add_index(PRICE, index_type="btree")
        rs = col.retrieve(gt(PRICE, 0))
        assert len(rs) == 0

    def test_float_range(self, products):
        """Float values work with btree range scans."""
        col, _ = products
        rs = col.retrieve(between(RATING, 3.0, 4.5))
        names = {p.name for p in rs}
        assert names == {"Widget", "Gadget", "Gizmo"}

    def test_negative_values(self):
        """Negative numbers are correctly ordered in BTree."""
        col = IndexedCollection()
        col.add_index(PRICE, index_type="btree")
        items = [
            Product("A", -10, 0),
            Product("B", -5, 0),
            Product("C", 0, 0),
            Product("D", 5, 0),
        ]
        col.add_many(items)
        rs = col.retrieve(lt(PRICE, 0))
        names = {p.name for p in rs}
        assert names == {"A", "B"}

    def test_remove_updates_btree(self, products):
        """Removing objects updates the BTree index correctly."""
        col, items = products
        gizmo = items[2]  # price=50
        col.remove(gizmo)
        rs = col.retrieve(between(PRICE, 40, 60))
        assert len(rs) == 0
