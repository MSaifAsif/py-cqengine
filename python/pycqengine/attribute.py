"""
Attribute extractors for indexed collections
"""

from typing import Any, Callable


class Attribute:
    """
    Defines an attribute extractor for objects in a collection.
    
    The extractor function is a lambda that extracts a specific attribute
    from an object, bypassing Python's dynamic attribute lookup during queries.
    
    Example:
        >>> class Car:
        ...     def __init__(self, vin, brand):
        ...         self.vin = vin
        ...         self.brand = brand
        ...
        >>> VIN = Attribute("vin", lambda car: car.vin)
        >>> BRAND = Attribute("brand", lambda car: car.brand)
    """
    
    def __init__(self, name: str, extractor: Callable[[Any], Any]):
        """
        Initialize an attribute extractor.
        
        Args:
            name: Unique name for this attribute
            extractor: Function that extracts the value from an object
        """
        self.name = name
        self.extractor = extractor
    
    def extract(self, obj: Any) -> Any:
        """
        Extract the attribute value from an object.
        
        Args:
            obj: Object to extract from
            
        Returns:
            The extracted value
        """
        return self.extractor(obj)
    
    def __repr__(self) -> str:
        return f"Attribute(name='{self.name}')"
    
    def __hash__(self) -> int:
        return hash(self.name)
    
    def __eq__(self, other: object) -> bool:
        if isinstance(other, Attribute):
            return self.name == other.name
        return False
