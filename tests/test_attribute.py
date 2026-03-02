"""
Unit tests for Attribute extractors
"""

import pytest
from pycqengine.attribute import Attribute


class TestAttribute:
    """Test attribute extractor functionality"""
    
    def test_create_attribute(self):
        """Test basic attribute creation"""
        attr = Attribute("name", lambda obj: obj.name)
        assert attr.name == "name"
        assert callable(attr.extractor)
    
    def test_extract_value(self):
        """Test extracting values from objects"""
        class Person:
            def __init__(self, name, age):
                self.name = name
                self.age = age
        
        NAME = Attribute("name", lambda p: p.name)
        AGE = Attribute("age", lambda p: p.age)
        
        person = Person("Alice", 30)
        assert NAME.extract(person) == "Alice"
        assert AGE.extract(person) == 30
    
    def test_extract_computed_value(self):
        """Test extracting computed values"""
        class Rectangle:
            def __init__(self, width, height):
                self.width = width
                self.height = height
        
        AREA = Attribute("area", lambda r: r.width * r.height)
        
        rect = Rectangle(5, 10)
        assert AREA.extract(rect) == 50
    
    def test_extract_nested_attribute(self):
        """Test extracting nested attributes"""
        class Address:
            def __init__(self, city):
                self.city = city
        
        class Person:
            def __init__(self, name, address):
                self.name = name
                self.address = address
        
        CITY = Attribute("city", lambda p: p.address.city)
        
        person = Person("Bob", Address("NYC"))
        assert CITY.extract(person) == "NYC"
    
    def test_attribute_equality(self):
        """Test attribute equality based on name"""
        attr1 = Attribute("name", lambda obj: obj.name)
        attr2 = Attribute("name", lambda obj: obj.value)  # Different extractor
        attr3 = Attribute("other", lambda obj: obj.name)
        
        assert attr1 == attr2  # Same name
        assert attr1 != attr3  # Different name
        assert attr1 != "name"  # Not an Attribute
    
    def test_attribute_hash(self):
        """Test attribute can be used as dict key"""
        attr1 = Attribute("name", lambda obj: obj.name)
        attr2 = Attribute("name", lambda obj: obj.value)
        
        d = {attr1: "value1"}
        assert d[attr2] == "value1"  # Same hash
    
    def test_attribute_repr(self):
        """Test string representation"""
        attr = Attribute("user_id", lambda obj: obj.id)
        assert repr(attr) == "Attribute(name='user_id')"
