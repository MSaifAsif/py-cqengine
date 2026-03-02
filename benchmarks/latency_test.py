"""
PyCQEngine Performance Benchmarks

Tests query latency on collections of 1M+ objects.
Target: Sub-100 microsecond latency for point lookups.
"""

import time
import random
import statistics
from typing import List

from pycqengine import IndexedCollection, Attribute, eq, and_, or_, in_


class Car:
    """Simple car object for benchmarking"""
    __slots__ = ['vin', 'brand', 'color', 'price', 'year']
    
    def __init__(self, vin: int, brand: str, color: str, price: int, year: int):
        self.vin = vin
        self.brand = brand
        self.color = color
        self.price = price
        self.year = year
    
    def __repr__(self):
        return f"Car({self.vin}, {self.brand}, {self.color}, ${self.price}, {self.year})"


# Define attributes
VIN = Attribute("vin", lambda c: c.vin)
BRAND = Attribute("brand", lambda c: c.brand)
COLOR = Attribute("color", lambda c: c.color)
PRICE = Attribute("price", lambda c: c.price)
YEAR = Attribute("year", lambda c: c.year)

# Test data parameters
BRANDS = ["Tesla", "Ford", "BMW", "Toyota", "Honda", "Mercedes", "Audi", "Nissan", "Chevrolet", "Volkswagen"]
COLORS = ["Red", "Blue", "Black", "White", "Silver", "Gray", "Green", "Yellow"]
YEARS = list(range(2015, 2026))


def generate_cars(n: int) -> List[Car]:
    """Generate n random car objects"""
    random.seed(42)  # Reproducible results
    cars = []
    for i in range(n):
        brand = random.choice(BRANDS)
        color = random.choice(COLORS)
        price = random.randint(20000, 100000)
        year = random.choice(YEARS)
        cars.append(Car(i, brand, color, price, year))
    return cars


def measure_latency(func, iterations: int = 100) -> dict:
    """
    Measure latency statistics for a function.
    
    Returns:
        Dict with min, max, mean, median, p95, p99 latencies in microseconds
    """
    latencies = []
    
    # Warmup
    for _ in range(10):
        func()
    
    # Actual measurements
    for _ in range(iterations):
        start = time.perf_counter_ns()
        func()
        end = time.perf_counter_ns()
        latencies.append((end - start) / 1000)  # Convert to microseconds
    
    latencies.sort()
    return {
        'min': latencies[0],
        'max': latencies[-1],
        'mean': statistics.mean(latencies),
        'median': statistics.median(latencies),
        'p95': latencies[int(len(latencies) * 0.95)],
        'p99': latencies[int(len(latencies) * 0.99)],
    }


def print_stats(name: str, stats: dict, target_us: float = None):
    """Print latency statistics"""
    print(f"\n{name}:")
    print(f"  Min:     {stats['min']:>8.2f} μs")
    print(f"  Median:  {stats['median']:>8.2f} μs")
    print(f"  Mean:    {stats['mean']:>8.2f} μs")
    print(f"  P95:     {stats['p95']:>8.2f} μs")
    print(f"  P99:     {stats['p99']:>8.2f} μs")
    print(f"  Max:     {stats['max']:>8.2f} μs")
    
    if target_us:
        if stats['median'] < target_us:
            print(f"  ✓ PASSED: Median {stats['median']:.2f}μs < target {target_us}μs")
        else:
            print(f"  ✗ FAILED: Median {stats['median']:.2f}μs >= target {target_us}μs")


def benchmark_vs_list_comprehension(cars_list: List[Car], cars_indexed: IndexedCollection):
    """Compare indexed query vs Python list comprehension"""
    print("\n" + "=" * 70)
    print("Benchmark: PyCQEngine vs List Comprehension")
    print("=" * 70)
    
    target_brand = "Tesla"
    
    # Indexed query
    def indexed_query():
        return list(cars_indexed.retrieve(eq(BRAND, target_brand)))
    
    # List comprehension
    def list_comp():
        return [c for c in cars_list if c.brand == target_brand]
    
    print(f"\nQuery: Find all cars with brand='{target_brand}'")
    print(f"Collection size: {len(cars_list):,} objects")
    
    indexed_stats = measure_latency(indexed_query, iterations=100)
    list_stats = measure_latency(list_comp, iterations=100)
    
    print_stats("PyCQEngine (Indexed)", indexed_stats)
    print_stats("List Comprehension", list_stats)
    
    speedup = list_stats['median'] / indexed_stats['median']
    print(f"\n  🚀 Speedup: {speedup:.1f}x faster")


def main():
    print("=" * 70)
    print("PyCQEngine Performance Benchmark Suite")
    print("=" * 70)
    print("\nTarget: Sub-100 microsecond latency for point lookups")
    print("Test configuration: 1,000,000 objects\n")
    
    # Generate test data
    print("Generating test data...")
    N = 1_000_000
    cars_list = generate_cars(N)
    print(f"✓ Generated {N:,} car objects")
    
    # Setup indexed collection
    print("\nBuilding indexed collection...")
    start = time.perf_counter()
    
    cars_indexed = IndexedCollection()
    cars_indexed.add_index(VIN)
    cars_indexed.add_index(BRAND)
    cars_indexed.add_index(COLOR)
    cars_indexed.add_index(PRICE)
    cars_indexed.add_index(YEAR)
    
    cars_indexed.add_many(cars_list)
    
    build_time = time.perf_counter() - start
    print(f"✓ Built collection with 5 indexes in {build_time:.2f}s")
    print(f"  Throughput: {N / build_time:,.0f} objects/sec")
    
    # Benchmark 1: Point lookup (best case scenario)
    print("\n" + "=" * 70)
    print("Benchmark 1: Point Lookup (Unique VIN)")
    print("=" * 70)
    print("\nQuery: eq(VIN, 500000)")
    print("Expected: 1 result (unique key lookup)")
    
    def point_lookup():
        return list(cars_indexed.retrieve(eq(VIN, 500000)))
    
    stats = measure_latency(point_lookup, iterations=1000)
    print_stats("Point Lookup", stats, target_us=100)
    
    # Verify result
    result = point_lookup()
    assert len(result) == 1
    assert result[0].vin == 500000
    print(f"  ✓ Verified: Found car with VIN={result[0].vin}")
    
    # Benchmark 2: Selective equality query
    print("\n" + "=" * 70)
    print("Benchmark 2: Selective Equality Query")
    print("=" * 70)
    print("\nQuery: eq(BRAND, 'Tesla')")
    
    def selective_query():
        return list(cars_indexed.retrieve(eq(BRAND, "Tesla")))
    
    stats = measure_latency(selective_query, iterations=100)
    result = selective_query()
    print(f"Expected: ~{N / len(BRANDS):,.0f} results ({len(result):,} actual)")
    print_stats("Equality Query", stats, target_us=200)
    
    # Benchmark 3: AND query (intersection)
    print("\n" + "=" * 70)
    print("Benchmark 3: AND Query (Intersection)")
    print("=" * 70)
    print("\nQuery: and_(eq(BRAND, 'Tesla'), eq(COLOR, 'Red'))")
    
    def and_query():
        return list(cars_indexed.retrieve(and_(
            eq(BRAND, "Tesla"),
            eq(COLOR, "Red")
        )))
    
    stats = measure_latency(and_query, iterations=100)
    result = and_query()
    print(f"Expected: ~{N / (len(BRANDS) * len(COLORS)):,.0f} results ({len(result):,} actual)")
    print_stats("AND Query", stats, target_us=300)
    
    # Benchmark 4: OR query (union)
    print("\n" + "=" * 70)
    print("Benchmark 4: OR Query (Union)")
    print("=" * 70)
    print("\nQuery: or_(eq(BRAND, 'Tesla'), eq(BRAND, 'BMW'))")
    
    def or_query():
        return list(cars_indexed.retrieve(or_(
            eq(BRAND, "Tesla"),
            eq(BRAND, "BMW")
        )))
    
    stats = measure_latency(or_query, iterations=100)
    result = or_query()
    print(f"Expected: ~{N * 2 / len(BRANDS):,.0f} results ({len(result):,} actual)")
    print_stats("OR Query", stats, target_us=400)
    
    # Benchmark 5: IN query (multiple values)
    print("\n" + "=" * 70)
    print("Benchmark 5: IN Query (Multiple Values)")
    print("=" * 70)
    print("\nQuery: in_(COLOR, ['Red', 'Blue', 'Black'])")
    
    def in_query():
        return list(cars_indexed.retrieve(in_(COLOR, ["Red", "Blue", "Black"])))
    
    stats = measure_latency(in_query, iterations=100)
    result = in_query()
    print(f"Expected: ~{N * 3 / len(COLORS):,.0f} results ({len(result):,} actual)")
    print_stats("IN Query", stats, target_us=500)
    
    # Comparison with list comprehension
    benchmark_vs_list_comprehension(cars_list, cars_indexed)


if __name__ == "__main__":
    main()
