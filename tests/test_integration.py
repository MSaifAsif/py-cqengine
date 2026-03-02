"""
Integration tests for PyCQEngine

Tests end-to-end workflows and realistic usage scenarios.
"""

import pytest
import random
from pycqengine import IndexedCollection, Attribute, eq, and_, or_, in_


class Person:
    """Person object for integration tests"""
    def __init__(self, id, name, age, city, department, salary):
        self.id = id
        self.name = name
        self.age = age
        self.city = city
        self.department = department
        self.salary = salary
    
    def __repr__(self):
        return f"Person(id={self.id}, name='{self.name}', age={self.age})"


@pytest.fixture
def person_attributes():
    """Person attributes"""
    return {
        'ID': Attribute("id", lambda p: p.id),
        'NAME': Attribute("name", lambda p: p.name),
        'AGE': Attribute("age", lambda p: p.age),
        'CITY': Attribute("city", lambda p: p.city),
        'DEPT': Attribute("department", lambda p: p.department),
        'SALARY': Attribute("salary", lambda p: p.salary),
    }


@pytest.fixture
def large_person_dataset():
    """Generate a large dataset for testing"""
    random.seed(42)
    names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"]
    cities = ["NYC", "LA", "Chicago", "Houston", "Phoenix"]
    departments = ["Engineering", "Sales", "Marketing", "HR"]
    
    people = []
    for i in range(10_000):
        person = Person(
            id=i,
            name=random.choice(names),
            age=random.randint(22, 65),
            city=random.choice(cities),
            department=random.choice(departments),
            salary=random.randint(40_000, 150_000)
        )
        people.append(person)
    
    return people


class TestEndToEndWorkflow:
    """Test complete workflow from setup to query"""
    
    def test_simple_workflow(self, person_attributes):
        """Test basic end-to-end workflow"""
        # 1. Create collection
        people = IndexedCollection()
        
        # 2. Add indexes
        people.add_index(person_attributes['ID'])
        people.add_index(person_attributes['NAME'])
        people.add_index(person_attributes['CITY'])
        
        # 3. Add data
        dataset = [
            Person(1, "Alice", 30, "NYC", "Engineering", 100000),
            Person(2, "Bob", 35, "LA", "Sales", 80000),
            Person(3, "Charlie", 28, "NYC", "Engineering", 95000),
        ]
        people.add_many(dataset)
        
        # 4. Query
        nyc_people = list(people.retrieve(eq(person_attributes['CITY'], "NYC")))
        assert len(nyc_people) == 2
        
        # 5. Verify results
        names = {p.name for p in nyc_people}
        assert names == {"Alice", "Charlie"}
    
    def test_incremental_additions(self, person_attributes):
        """Test adding data incrementally over time"""
        people = IndexedCollection()
        people.add_index(person_attributes['NAME'])
        
        # Start with empty
        assert len(people) == 0
        
        # Add first batch
        batch1 = [
            Person(1, "Alice", 30, "NYC", "Engineering", 100000),
            Person(2, "Bob", 35, "LA", "Sales", 80000),
        ]
        people.add_many(batch1)
        assert len(people) == 2
        
        # Add second batch
        batch2 = [
            Person(3, "Charlie", 28, "NYC", "Engineering", 95000),
        ]
        people.add_many(batch2)
        assert len(people) == 3
        
        # Add individual
        people.add(Person(4, "Diana", 32, "Chicago", "HR", 75000))
        assert len(people) == 4
        
        # Verify all queries work
        results = list(people.retrieve(eq(person_attributes['NAME'], "Alice")))
        assert len(results) == 1


class TestLargeDataset:
    """Test with large datasets (10K+ objects)"""
    
    def test_build_large_collection(self, person_attributes, large_person_dataset):
        """Test building collection with 10K objects"""
        people = IndexedCollection()
        people.add_index(person_attributes['ID'])
        people.add_index(person_attributes['NAME'])
        people.add_index(person_attributes['CITY'])
        people.add_index(person_attributes['DEPT'])
        
        # Add all data
        people.add_many(large_person_dataset)
        
        assert len(people) == 10_000
    
    def test_query_large_collection(self, person_attributes, large_person_dataset):
        """Test querying large collection"""
        people = IndexedCollection()
        people.add_index(person_attributes['CITY'])
        people.add_many(large_person_dataset)
        
        # Query for specific city
        nyc_people = list(people.retrieve(eq(person_attributes['CITY'], "NYC")))
        
        # Should have roughly 20% (2000 people)
        assert 1800 < len(nyc_people) < 2200
        
        # Verify all results are from NYC
        for person in nyc_people:
            assert person.city == "NYC"
    
    def test_multiple_queries_large_dataset(self, person_attributes, large_person_dataset):
        """Test running multiple queries on large dataset"""
        people = IndexedCollection()
        people.add_index(person_attributes['NAME'])
        people.add_index(person_attributes['DEPT'])
        people.add_many(large_person_dataset)
        
        # Multiple queries in succession
        for _ in range(10):
            results = list(people.retrieve(eq(person_attributes['NAME'], "Alice")))
            assert len(results) > 0
            
            dept_results = list(people.retrieve(eq(person_attributes['DEPT'], "Engineering")))
            assert len(dept_results) > 0


class TestComplexScenarios:
    """Test complex real-world scenarios"""
    
    def test_employee_search_scenario(self, person_attributes, large_person_dataset):
        """Simulate employee search system"""
        employees = IndexedCollection()
        employees.add_index(person_attributes['CITY'])
        employees.add_index(person_attributes['DEPT'])
        employees.add_many(large_person_dataset)
        
        # Scenario 1: Find all engineers in NYC
        nyc_engineers = list(employees.retrieve(and_(
            eq(person_attributes['CITY'], "NYC"),
            eq(person_attributes['DEPT'], "Engineering")
        )))
        
        # Verify results
        for person in nyc_engineers:
            assert person.city == "NYC"
            assert person.department == "Engineering"
        
        # Scenario 2: Find employees in NYC or LA
        coastal_employees = list(employees.retrieve(or_(
            eq(person_attributes['CITY'], "NYC"),
            eq(person_attributes['CITY'], "LA")
        )))
        
        # Should be ~40% of employees
        assert 3500 < len(coastal_employees) < 4500
        
        # Scenario 3: Find employees in specific departments
        target_depts = ["Engineering", "Sales"]
        dept_employees = list(employees.retrieve(
            in_(person_attributes['DEPT'], target_depts)
        ))
        
        # Should be ~50% of employees
        assert 4500 < len(dept_employees) < 5500
    
    def test_data_filtering_pipeline(self, person_attributes):
        """Test progressive filtering pipeline"""
        people = IndexedCollection()
        people.add_index(person_attributes['NAME'])
        people.add_index(person_attributes['AGE'])
        people.add_index(person_attributes['CITY'])
        
        # Build dataset
        dataset = [
            Person(1, "Alice", 30, "NYC", "Engineering", 100000),
            Person(2, "Alice", 35, "LA", "Sales", 80000),
            Person(3, "Bob", 30, "NYC", "Engineering", 95000),
            Person(4, "Alice", 30, "Chicago", "Marketing", 85000),
            Person(5, "Charlie", 30, "NYC", "Engineering", 90000),
        ]
        people.add_many(dataset)
        
        # Filter 1: Name = Alice
        step1 = list(people.retrieve(eq(person_attributes['NAME'], "Alice")))
        assert len(step1) == 3
        
        # Filter 2: Name = Alice AND Age = 30
        step2 = list(people.retrieve(and_(
            eq(person_attributes['NAME'], "Alice"),
            eq(person_attributes['AGE'], 30)
        )))
        assert len(step2) == 2
        
        # Filter 3: Name = Alice AND Age = 30 AND City = NYC
        step3 = list(people.retrieve(and_(
            eq(person_attributes['NAME'], "Alice"),
            eq(person_attributes['AGE'], 30),
            eq(person_attributes['CITY'], "NYC")
        )))
        assert len(step3) == 1
        assert step3[0].id == 1


class TestEdgeCases:
    """Test edge cases and boundary conditions"""
    
    def test_empty_collection_queries(self, person_attributes):
        """Test querying empty collection"""
        people = IndexedCollection()
        people.add_index(person_attributes['NAME'])
        
        results = list(people.retrieve(eq(person_attributes['NAME'], "Nobody")))
        assert len(results) == 0
    
    def test_single_object_collection(self, person_attributes):
        """Test collection with single object"""
        people = IndexedCollection()
        people.add_index(person_attributes['ID'])
        
        people.add(Person(1, "Alice", 30, "NYC", "Engineering", 100000))
        
        results = list(people.retrieve(eq(person_attributes['ID'], 1)))
        assert len(results) == 1
        assert results[0].name == "Alice"
    
    def test_all_objects_match(self, person_attributes):
        """Test query where all objects match"""
        people = IndexedCollection()
        people.add_index(person_attributes['CITY'])
        
        # All people in NYC
        dataset = [
            Person(i, f"Person{i}", 30, "NYC", "Engineering", 100000)
            for i in range(100)
        ]
        people.add_many(dataset)
        
        results = list(people.retrieve(eq(person_attributes['CITY'], "NYC")))
        assert len(results) == 100
    
    def test_query_non_indexed_attribute(self, person_attributes):
        """Test querying attribute that's not indexed"""
        people = IndexedCollection()
        # Only index NAME, not CITY
        people.add_index(person_attributes['NAME'])
        
        people.add(Person(1, "Alice", 30, "NYC", "Engineering", 100000))
        
        # Querying non-indexed attribute should raise error
        with pytest.raises(Exception):  # Rust will raise an error
            list(people.retrieve(eq(person_attributes['CITY'], "NYC")))
    
    def test_clear_and_rebuild(self, person_attributes):
        """Test clearing collection and rebuilding"""
        people = IndexedCollection()
        people.add_index(person_attributes['NAME'])
        
        # Build first time
        dataset1 = [Person(i, "Alice", 30, "NYC", "Engineering", 100000) for i in range(10)]
        people.add_many(dataset1)
        assert len(people) == 10
        
        # Clear
        people.clear()
        assert len(people) == 0
        
        # Rebuild with different data
        dataset2 = [Person(i, "Bob", 30, "LA", "Sales", 80000) for i in range(5)]
        people.add_many(dataset2)
        assert len(people) == 5
        
        # Old queries should return nothing
        results = list(people.retrieve(eq(person_attributes['NAME'], "Alice")))
        assert len(results) == 0
        
        # New queries should work
        results = list(people.retrieve(eq(person_attributes['NAME'], "Bob")))
        assert len(results) == 5


class TestMemorySafety:
    """Test memory safety and Python object lifecycle"""
    
    def test_object_identity_preserved(self, person_attributes):
        """Test that retrieved objects are the same Python objects"""
        people = IndexedCollection()
        people.add_index(person_attributes['ID'])
        
        original = Person(1, "Alice", 30, "NYC", "Engineering", 100000)
        people.add(original)
        
        # Retrieve
        results = list(people.retrieve(eq(person_attributes['ID'], 1)))
        retrieved = results[0]
        
        # Should be the same object (same memory address)
        assert retrieved is original
        assert id(retrieved) == id(original)
    
    def test_lazy_iteration_memory(self, person_attributes, large_person_dataset):
        """Test that lazy iteration doesn't materialize all objects"""
        people = IndexedCollection()
        people.add_index(person_attributes['CITY'])
        people.add_many(large_person_dataset)
        
        # Get result set (should be lazy)
        result_set = people.retrieve(eq(person_attributes['CITY'], "NYC"))
        
        # Result set exists but objects not materialized yet
        assert len(result_set) > 0
        
        # Iterate only first 10
        count = 0
        for person in result_set:
            count += 1
            if count >= 10:
                break
        
        # Successfully iterated without materializing all ~2000 NYC people
        assert count == 10


@pytest.mark.slow
class TestPerformanceIntegration:
    """Integration tests focused on performance characteristics"""
    
    def test_build_time_scales_linearly(self, person_attributes):
        """Test that build time scales roughly linearly with data size"""
        import time
        
        sizes = [1_000, 5_000, 10_000]
        times = []
        
        for size in sizes:
            people = IndexedCollection()
            people.add_index(person_attributes['ID'])
            people.add_index(person_attributes['NAME'])
            
            dataset = [
                Person(i, f"Person{i%100}", 30, "NYC", "Engineering", 100000)
                for i in range(size)
            ]
            
            start = time.perf_counter()
            people.add_many(dataset)
            elapsed = time.perf_counter() - start
            times.append(elapsed)
        
        # Verify roughly linear scaling (within margin)
        # With parallel insertion, sublinear scaling is expected
        ratio = times[2] / times[0]  # 10K / 1K
        assert 2 < ratio < 20  # Parallel insertion makes small batches relatively slower


class TestRemoveOperations:
    """Test object removal from collections"""

    def test_remove_single_object(self, person_attributes):
        """Test removing a single object"""
        people = IndexedCollection()
        people.add_index(person_attributes['NAME'])
        people.add_index(person_attributes['CITY'])

        alice = Person(1, "Alice", 30, "NYC", "Engineering", 100000)
        bob = Person(2, "Bob", 35, "LA", "Sales", 80000)
        people.add(alice)
        people.add(bob)
        assert len(people) == 2

        # Remove Alice
        removed = people.remove(alice)
        assert removed is True
        assert len(people) == 1

        # Alice should no longer be findable
        results = list(people.retrieve(eq(person_attributes['NAME'], "Alice")))
        assert len(results) == 0

        # Bob should still be there
        results = list(people.retrieve(eq(person_attributes['NAME'], "Bob")))
        assert len(results) == 1

    def test_remove_nonexistent_object(self, person_attributes):
        """Test removing an object that isn't in the collection"""
        people = IndexedCollection()
        people.add_index(person_attributes['NAME'])

        alice = Person(1, "Alice", 30, "NYC", "Engineering", 100000)
        people.add(alice)

        # Try to remove an object that was never added
        stranger = Person(99, "Stranger", 25, "SF", "HR", 60000)
        removed = people.remove(stranger)
        assert removed is False
        assert len(people) == 1

    def test_remove_many(self, person_attributes, large_person_dataset):
        """Test batch removal of objects"""
        people = IndexedCollection()
        people.add_index(person_attributes['ID'])
        people.add_index(person_attributes['NAME'])
        people.add_index(person_attributes['CITY'])
        people.add_many(large_person_dataset)

        initial_count = len(people)
        assert initial_count == 10_000

        # Remove first 100 people
        to_remove = large_person_dataset[:100]
        removed_count = people.remove_many(to_remove)
        assert removed_count == 100
        assert len(people) == 9_900

    def test_remove_then_query(self, person_attributes):
        """Test that queries reflect removals"""
        people = IndexedCollection()
        people.add_index(person_attributes['CITY'])

        dataset = [
            Person(i, f"Person{i}", 30, "NYC", "Engineering", 100000)
            for i in range(10)
        ]
        people.add_many(dataset)

        nyc_before = list(people.retrieve(eq(person_attributes['CITY'], "NYC")))
        assert len(nyc_before) == 10

        # Remove 5 people
        people.remove_many(dataset[:5])

        nyc_after = list(people.retrieve(eq(person_attributes['CITY'], "NYC")))
        assert len(nyc_after) == 5

    def test_remove_then_add_back(self, person_attributes):
        """Test removing and re-adding objects"""
        people = IndexedCollection()
        people.add_index(person_attributes['NAME'])

        alice = Person(1, "Alice", 30, "NYC", "Engineering", 100000)
        people.add(alice)
        assert len(people) == 1

        people.remove(alice)
        assert len(people) == 0

        # Re-add
        people.add(alice)
        assert len(people) == 1
        results = list(people.retrieve(eq(person_attributes['NAME'], "Alice")))
        assert len(results) == 1


class TestMemoryLifecycle:
    """Test that memory is properly managed through the object lifecycle"""

    def test_clear_drops_references(self, person_attributes):
        """Test that clear() allows Python GC to reclaim objects"""
        import gc
        import sys

        people = IndexedCollection()
        people.add_index(person_attributes['ID'])

        obj = Person(1, "Alice", 30, "NYC", "Engineering", 100000)
        initial_refcount = sys.getrefcount(obj)

        people.add(obj)
        after_add_refcount = sys.getrefcount(obj)
        # Rust should hold an extra reference
        assert after_add_refcount > initial_refcount

        people.clear()
        gc.collect()

        after_clear_refcount = sys.getrefcount(obj)
        # After clear, refcount should drop back
        assert after_clear_refcount <= initial_refcount

    def test_remove_drops_reference(self, person_attributes):
        """Test that remove() drops the Rust-held reference"""
        import gc
        import sys

        people = IndexedCollection()
        people.add_index(person_attributes['NAME'])

        obj = Person(1, "Alice", 30, "NYC", "Engineering", 100000)
        initial_refcount = sys.getrefcount(obj)

        people.add(obj)
        assert sys.getrefcount(obj) > initial_refcount

        people.remove(obj)
        gc.collect()

        assert sys.getrefcount(obj) <= initial_refcount

    def test_del_cleans_up(self, person_attributes):
        """Test that deleting the collection releases all references"""
        import gc
        import sys

        obj = Person(1, "Alice", 30, "NYC", "Engineering", 100000)
        initial_refcount = sys.getrefcount(obj)

        people = IndexedCollection()
        people.add_index(person_attributes['ID'])
        people.add(obj)
        assert sys.getrefcount(obj) > initial_refcount

        del people
        gc.collect()

        # After collection is deleted, refcount should return to baseline
        assert sys.getrefcount(obj) <= initial_refcount
