"""
Unit tests for IndexedCollection
"""

import pytest
from pycqengine import IndexedCollection, Attribute, eq


class Car:
    """Test object"""
    def __init__(self, vin, brand, price):
        self.vin = vin
        self.brand = brand
        self.price = price


@pytest.fixture
def attributes():
    """Create test attributes"""
    return {
        'VIN': Attribute("vin", lambda c: c.vin),
        'BRAND': Attribute("brand", lambda c: c.brand),
        'PRICE': Attribute("price", lambda c: c.price),
    }


@pytest.fixture
def empty_collection(attributes):
    """Create empty indexed collection"""
    collection = IndexedCollection()
    collection.add_index(attributes['VIN'])
    collection.add_index(attributes['BRAND'])
    return collection


@pytest.fixture
def sample_cars():
    """Create sample car objects"""
    return [
        Car(1, "Tesla", 50000),
        Car(2, "Ford", 30000),
        Car(3, "Tesla", 60000),
        Car(4, "BMW", 45000),
    ]


class TestIndexedCollection:
    """Test IndexedCollection functionality"""
    
    def test_create_empty_collection(self):
        """Test creating an empty collection"""
        collection = IndexedCollection()
        assert len(collection) == 0
    
    def test_add_index(self, empty_collection, attributes):
        """Test adding indexes"""
        # Already has VIN and BRAND from fixture
        empty_collection.add_index(attributes['PRICE'])
        # No error means success
    
    def test_add_single_object(self, empty_collection, attributes):
        """Test adding a single object"""
        car = Car(1, "Tesla", 50000)
        empty_collection.add(car)
        assert len(empty_collection) == 1
        
        # Verify we can retrieve it
        results = list(empty_collection.retrieve(eq(attributes['VIN'], 1)))
        assert len(results) == 1
        assert results[0].vin == 1
    
    def test_add_many(self, empty_collection, sample_cars):
        """Test batch adding objects"""
        empty_collection.add_many(sample_cars)
        assert len(empty_collection) == 4
    
    def test_retrieve_by_unique_key(self, empty_collection, attributes, sample_cars):
        """Test retrieving by unique key (VIN)"""
        empty_collection.add_many(sample_cars)
        
        results = list(empty_collection.retrieve(eq(attributes['VIN'], 2)))
        assert len(results) == 1
        assert results[0].vin == 2
        assert results[0].brand == "Ford"
    
    def test_retrieve_by_non_unique_key(self, empty_collection, attributes, sample_cars):
        """Test retrieving by non-unique key (Brand)"""
        empty_collection.add_many(sample_cars)
        
        results = list(empty_collection.retrieve(eq(attributes['BRAND'], "Tesla")))
        assert len(results) == 2
        vins = {r.vin for r in results}
        assert vins == {1, 3}
    
    def test_retrieve_no_results(self, empty_collection, attributes, sample_cars):
        """Test query with no matches"""
        empty_collection.add_many(sample_cars)
        
        results = list(empty_collection.retrieve(eq(attributes['BRAND'], "NonExistent")))
        assert len(results) == 0
    
    def test_clear_collection(self, empty_collection, sample_cars):
        """Test clearing the collection"""
        empty_collection.add_many(sample_cars)
        assert len(empty_collection) == 4
        
        empty_collection.clear()
        assert len(empty_collection) == 0
    
    def test_add_object_without_index(self):
        """Test adding object with attribute not indexed"""
        collection = IndexedCollection()
        VIN = Attribute("vin", lambda c: c.vin)
        collection.add_index(VIN)
        
        # Add car but BRAND is not indexed
        car = Car(1, "Tesla", 50000)
        collection.add(car)  # Should not fail
        assert len(collection) == 1
    
    def test_lazy_result_set(self, empty_collection, attributes, sample_cars):
        """Test that ResultSet is lazy"""
        empty_collection.add_many(sample_cars)
        
        result_set = empty_collection.retrieve(eq(attributes['BRAND'], "Tesla"))
        
        # ResultSet created but not materialized
        assert len(result_set) == 2
        
        # Now materialize
        results = list(result_set)
        assert len(results) == 2
    
    def test_multiple_queries_same_collection(self, empty_collection, attributes, sample_cars):
        """Test running multiple queries on same collection"""
        empty_collection.add_many(sample_cars)
        
        # First query
        teslas = list(empty_collection.retrieve(eq(attributes['BRAND'], "Tesla")))
        assert len(teslas) == 2
        
        # Second query
        fords = list(empty_collection.retrieve(eq(attributes['BRAND'], "Ford")))
        assert len(fords) == 1
        
        # Third query
        bmws = list(empty_collection.retrieve(eq(attributes['BRAND'], "BMW")))
        assert len(bmws) == 1
    
    def test_add_objects_incrementally(self, empty_collection, attributes):
        """Test adding objects one by one"""
        cars = [
            Car(1, "Tesla", 50000),
            Car(2, "Ford", 30000),
            Car(3, "BMW", 45000),
        ]
        
        for car in cars:
            empty_collection.add(car)
        
        assert len(empty_collection) == 3
        
        # Verify all are retrievable
        all_brands = ["Tesla", "Ford", "BMW"]
        for brand in all_brands:
            results = list(empty_collection.retrieve(eq(attributes['BRAND'], brand)))
            assert len(results) == 1
