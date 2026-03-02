"""
Quick smoke test to verify basic functionality
"""

from pycqengine import IndexedCollection, Attribute, eq, and_, or_, in_

class Car:
    def __init__(self, vin, brand, price):
        self.vin = vin
        self.brand = brand
        self.price = price
    
    def __repr__(self):
        return f"Car(vin={self.vin}, brand='{self.brand}', price={self.price})"

# Define attributes
VIN = Attribute("vin", lambda c: c.vin)
BRAND = Attribute("brand", lambda c: c.brand)
PRICE = Attribute("price", lambda c: c.price)

# Create collection
cars = IndexedCollection()
cars.add_index(VIN)
cars.add_index(BRAND)
cars.add_index(PRICE)

# Add test data
test_cars = [
    Car(1, "Tesla", 50000),
    Car(2, "Ford", 30000),
    Car(3, "Tesla", 60000),
    Car(4, "BMW", 45000),
    Car(5, "Ford", 35000),
]

print("Adding cars...")
cars.add_many(test_cars)
print(f"✓ Added {len(cars)} cars to collection\n")

# Test 1: Equality query
print("Test 1: Find all Teslas")
results = list(cars.retrieve(eq(BRAND, "Tesla")))
print(f"  Found {len(results)} Teslas:")
for car in results:
    print(f"    {car}")
assert len(results) == 2, "Expected 2 Teslas"
print("  ✓ Test passed\n")

# Test 2: AND query
print("Test 2: Find Fords")
results = list(cars.retrieve(eq(BRAND, "Ford")))
print(f"  Found {len(results)} Fords:")
for car in results:
    print(f"    {car}")
assert len(results) == 2, "Expected 2 Fords"
print("  ✓ Test passed\n")

# Test 3: OR query
print("Test 3: Find Tesla OR BMW")
results = list(cars.retrieve(or_(
    eq(BRAND, "Tesla"),
    eq(BRAND, "BMW")
)))
print(f"  Found {len(results)} results:")
for car in results:
    print(f"    {car}")
assert len(results) == 3, "Expected 3 results (2 Tesla + 1 BMW)"
print("  ✓ Test passed\n")

# Test 4: IN query
print("Test 4: Find brands in [Tesla, Ford]")
results = list(cars.retrieve(in_(BRAND, ["Tesla", "Ford"])))
print(f"  Found {len(results)} results:")
for car in results:
    print(f"    {car}")
assert len(results) == 4, "Expected 4 results"
print("  ✓ Test passed\n")

# Test 5: Specific VIN lookup
print("Test 5: Find car with VIN=3")
results = list(cars.retrieve(eq(VIN, 3)))
print(f"  Found {len(results)} result:")
for car in results:
    print(f"    {car}")
assert len(results) == 1, "Expected 1 result"
assert results[0].vin == 3, "Expected VIN 3"
print("  ✓ Test passed\n")

print("=" * 50)
print("✓ All tests passed!")
print("=" * 50)
