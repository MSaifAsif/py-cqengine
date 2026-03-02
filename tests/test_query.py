"""
Unit tests for Query DSL
"""

import pytest
from pycqengine import IndexedCollection, Attribute, eq, and_, or_, in_
from pycqengine.query import EqualityQuery, AndQuery, OrQuery, InQuery


class Car:
    """Test object"""
    def __init__(self, vin, brand, color, price):
        self.vin = vin
        self.brand = brand
        self.color = color
        self.price = price


@pytest.fixture
def attributes():
    """Create test attributes"""
    return {
        'VIN': Attribute("vin", lambda c: c.vin),
        'BRAND': Attribute("brand", lambda c: c.brand),
        'COLOR': Attribute("color", lambda c: c.color),
        'PRICE': Attribute("price", lambda c: c.price),
    }


@pytest.fixture
def collection_with_data(attributes):
    """Create collection with test data"""
    collection = IndexedCollection()
    collection.add_index(attributes['VIN'])
    collection.add_index(attributes['BRAND'])
    collection.add_index(attributes['COLOR'])
    collection.add_index(attributes['PRICE'])
    
    cars = [
        Car(1, "Tesla", "Red", 50000),
        Car(2, "Ford", "Blue", 30000),
        Car(3, "Tesla", "Blue", 60000),
        Car(4, "BMW", "Red", 45000),
        Car(5, "Ford", "Red", 35000),
    ]
    
    collection.add_many(cars)
    return collection


class TestEqualityQuery:
    """Test equality queries (eq)"""
    
    def test_eq_factory_function(self, attributes):
        """Test eq() factory function"""
        query = eq(attributes['BRAND'], "Tesla")
        assert isinstance(query, EqualityQuery)
        assert query.attribute == attributes['BRAND']
        assert query.value == "Tesla"
    
    def test_eq_single_result(self, collection_with_data, attributes):
        """Test equality query with single result"""
        results = list(collection_with_data.retrieve(eq(attributes['VIN'], 1)))
        assert len(results) == 1
        assert results[0].vin == 1
    
    def test_eq_multiple_results(self, collection_with_data, attributes):
        """Test equality query with multiple results"""
        results = list(collection_with_data.retrieve(eq(attributes['BRAND'], "Tesla")))
        assert len(results) == 2
        vins = {r.vin for r in results}
        assert vins == {1, 3}
    
    def test_eq_no_results(self, collection_with_data, attributes):
        """Test equality query with no matches"""
        results = list(collection_with_data.retrieve(eq(attributes['BRAND'], "NonExistent")))
        assert len(results) == 0
    
    def test_eq_different_types(self, collection_with_data, attributes):
        """Test equality with different data types"""
        # String
        string_results = list(collection_with_data.retrieve(eq(attributes['BRAND'], "Tesla")))
        assert len(string_results) == 2
        
        # Integer
        int_results = list(collection_with_data.retrieve(eq(attributes['VIN'], 3)))
        assert len(int_results) == 1
        assert int_results[0].vin == 3
        
        # Integer (price)
        price_results = list(collection_with_data.retrieve(eq(attributes['PRICE'], 50000)))
        assert len(price_results) == 1
        assert price_results[0].price == 50000


class TestAndQuery:
    """Test AND queries (intersection)"""
    
    def test_and_factory_function(self, attributes):
        """Test and_() factory function"""
        query = and_(
            eq(attributes['BRAND'], "Tesla"),
            eq(attributes['COLOR'], "Red")
        )
        assert isinstance(query, AndQuery)
        assert len(query.queries) == 2
    
    def test_and_two_conditions(self, collection_with_data, attributes):
        """Test AND with two conditions"""
        results = list(collection_with_data.retrieve(and_(
            eq(attributes['BRAND'], "Tesla"),
            eq(attributes['COLOR'], "Red")
        )))
        assert len(results) == 1
        assert results[0].vin == 1
    
    def test_and_no_intersection(self, collection_with_data, attributes):
        """Test AND with no common results"""
        results = list(collection_with_data.retrieve(and_(
            eq(attributes['BRAND'], "Tesla"),
            eq(attributes['COLOR'], "Green")  # No green Teslas
        )))
        assert len(results) == 0
    
    def test_and_three_conditions(self, collection_with_data, attributes):
        """Test AND with three conditions"""
        results = list(collection_with_data.retrieve(and_(
            eq(attributes['BRAND'], "Tesla"),
            eq(attributes['COLOR'], "Red"),
            eq(attributes['PRICE'], 50000)
        )))
        assert len(results) == 1
        assert results[0].vin == 1
    
    def test_and_empty_raises_error(self):
        """Test AND with no sub-queries raises error"""
        with pytest.raises(ValueError, match="requires at least one"):
            and_()


class TestOrQuery:
    """Test OR queries (union)"""
    
    def test_or_factory_function(self, attributes):
        """Test or_() factory function"""
        query = or_(
            eq(attributes['BRAND'], "Tesla"),
            eq(attributes['BRAND'], "BMW")
        )
        assert isinstance(query, OrQuery)
        assert len(query.queries) == 2
    
    def test_or_two_conditions(self, collection_with_data, attributes):
        """Test OR with two conditions"""
        results = list(collection_with_data.retrieve(or_(
            eq(attributes['BRAND'], "Tesla"),
            eq(attributes['BRAND'], "BMW")
        )))
        assert len(results) == 3
        vins = {r.vin for r in results}
        assert vins == {1, 3, 4}
    
    def test_or_overlapping_results(self, collection_with_data, attributes):
        """Test OR with overlapping results (no duplicates)"""
        results = list(collection_with_data.retrieve(or_(
            eq(attributes['COLOR'], "Red"),
            eq(attributes['BRAND'], "Tesla")  # Tesla VIN=1 is Red
        )))
        # Should include: VIN 1 (Tesla Red), VIN 3 (Tesla Blue), VIN 4 (BMW Red), VIN 5 (Ford Red)
        assert len(results) == 4
        vins = {r.vin for r in results}
        assert vins == {1, 3, 4, 5}
    
    def test_or_three_conditions(self, collection_with_data, attributes):
        """Test OR with three conditions"""
        results = list(collection_with_data.retrieve(or_(
            eq(attributes['BRAND'], "Tesla"),
            eq(attributes['BRAND'], "BMW"),
            eq(attributes['BRAND'], "Ford")
        )))
        assert len(results) == 5  # All cars
    
    def test_or_empty_raises_error(self):
        """Test OR with no sub-queries raises error"""
        with pytest.raises(ValueError, match="requires at least one"):
            or_()


class TestInQuery:
    """Test IN queries (membership)"""
    
    def test_in_factory_function(self, attributes):
        """Test in_() factory function"""
        query = in_(attributes['BRAND'], ["Tesla", "BMW"])
        assert isinstance(query, InQuery)
        # Values stored as list for Rust compatibility
        assert set(query.values) == {"Tesla", "BMW"}
    
    def test_in_multiple_values(self, collection_with_data, attributes):
        """Test IN with multiple values"""
        results = list(collection_with_data.retrieve(
            in_(attributes['BRAND'], ["Tesla", "BMW"])
        ))
        assert len(results) == 3
        vins = {r.vin for r in results}
        assert vins == {1, 3, 4}
    
    def test_in_single_value(self, collection_with_data, attributes):
        """Test IN with single value (equivalent to eq)"""
        results = list(collection_with_data.retrieve(
            in_(attributes['BRAND'], ["Tesla"])
        ))
        assert len(results) == 2
        vins = {r.vin for r in results}
        assert vins == {1, 3}
    
    def test_in_no_matches(self, collection_with_data, attributes):
        """Test IN with no matching values"""
        results = list(collection_with_data.retrieve(
            in_(attributes['BRAND'], ["Audi", "Mercedes"])
        ))
        assert len(results) == 0
    
    def test_in_with_set(self, collection_with_data, attributes):
        """Test IN with set input"""
        results = list(collection_with_data.retrieve(
            in_(attributes['COLOR'], {"Red", "Blue"})
        ))
        assert len(results) == 5  # All cars are either Red or Blue


class TestComplexQueries:
    """Test complex nested queries"""
    
    def test_and_of_ors(self, collection_with_data, attributes):
        """Test AND of OR queries"""
        results = list(collection_with_data.retrieve(and_(
            or_(
                eq(attributes['BRAND'], "Tesla"),
                eq(attributes['BRAND'], "Ford")
            ),
            eq(attributes['COLOR'], "Red")
        )))
        # Tesla Red (VIN 1) or Ford Red (VIN 5)
        assert len(results) == 2
        vins = {r.vin for r in results}
        assert vins == {1, 5}
    
    def test_or_of_ands(self, collection_with_data, attributes):
        """Test OR of AND queries"""
        results = list(collection_with_data.retrieve(or_(
            and_(
                eq(attributes['BRAND'], "Tesla"),
                eq(attributes['COLOR'], "Red")
            ),
            and_(
                eq(attributes['BRAND'], "BMW"),
                eq(attributes['COLOR'], "Red")
            )
        )))
        # Tesla Red (VIN 1) or BMW Red (VIN 4)
        assert len(results) == 2
        vins = {r.vin for r in results}
        assert vins == {1, 4}
    
    def test_query_repr(self, attributes):
        """Test query string representations"""
        eq_query = eq(attributes['BRAND'], "Tesla")
        assert "eq(brand, 'Tesla')" in repr(eq_query)
        
        and_query = and_(
            eq(attributes['BRAND'], "Tesla"),
            eq(attributes['COLOR'], "Red")
        )
        assert "and_" in repr(and_query)
