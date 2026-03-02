#!/usr/bin/env python3
"""
Basic PyCQEngine Benchmark - Simple AND Query Tests
Tests AND query performance at different scales: 10K, 100K, 500K objects
"""

import time
import statistics
from typing import List
from pycqengine import IndexedCollection, Attribute, eq, and_


class Car:
    """Simple car object for benchmarking"""
    __slots__ = ['vin', 'brand', 'color', 'price', 'year']
    
    def __init__(self, vin: int, brand: str, color: str, price: int, year: int):
        self.vin = vin
        self.brand = brand
        self.color = color
        self.price = price
        self.year = year


# Define attributes
VIN = Attribute("vin", lambda c: c.vin)
BRAND = Attribute("brand", lambda c: c.brand)
COLOR = Attribute("color", lambda c: c.color)
PRICE = Attribute("price", lambda c: c.price)
YEAR = Attribute("year", lambda c: c.year)


def generate_cars(n: int) -> List[Car]:
    """Generate n car objects with predictable distribution"""
    brands = ["Tesla", "Ford", "BMW", "Toyota", "Honda", "Mercedes", "Audi", "Nissan"]
    colors = ["Red", "Blue", "Black", "White", "Silver", "Gray"]
    years = [2020, 2021, 2022, 2023, 2024]
    
    cars = []
    for i in range(n):
        cars.append(Car(
            vin=i,
            brand=brands[i % len(brands)],
            color=colors[i % len(colors)],
            price=20000 + (i % 100) * 500,
            year=years[i % len(years)]
        ))
    return cars


def bench(func, iterations: int = 100) -> float:
    """Return median time in microseconds"""
    times = []
    
    # Warmup
    for _ in range(10):
        func()
    
    # Measure
    for _ in range(iterations):
        start = time.perf_counter()
        func()
        end = time.perf_counter()
        times.append((end - start) * 1_000_000)
    
    return statistics.median(times)


def run_test(n: int, test_name: str):
    """Run AND query test for a specific dataset size"""
    print("=" * 80)
    print(f"{test_name}: {n:,} objects")
    print("=" * 80)
    
    # Generate data
    print(f"Generating {n:,} cars...")
    cars = generate_cars(n)
    
    # Build indexed collection
    print(f"Building indexed collection...")
    start = time.perf_counter()
    
    collection = IndexedCollection()
    collection.add_index(VIN)
    collection.add_index(BRAND)
    collection.add_index(COLOR)
    collection.add_index(PRICE)
    collection.add_index(YEAR)
    collection.add_many(cars)
    
    build_time = time.perf_counter() - start
    print(f"✓ Indexed {len(collection):,} cars in {build_time:.2f}s ({n/build_time:,.0f} obj/s)\n")
    
    # Test 1: Simple AND query (Brand AND Color)
    print("Query: eq(BRAND, 'Tesla') AND eq(COLOR, 'Red')")
    
    def pycq_query():
        return list(collection.retrieve(and_(
            eq(BRAND, "Tesla"),
            eq(COLOR, "Red")
        )))
    
    def python_query():
        return [c for c in cars if c.brand == "Tesla" and c.color == "Red"]
    
    pycq_time = bench(pycq_query, iterations=100)
    pycq_results = pycq_query()
    
    python_time = bench(python_query, iterations=50)
    python_results = python_query()
    
    speedup = python_time / pycq_time
    
    print(f"Results: {len(pycq_results):,} cars")
    print(f"PyCQEngine:      {pycq_time:>10.2f} μs")
    print(f"Python baseline: {python_time:>10.2f} μs")
    print(f"Speedup:         {speedup:>10.1f}x")
    print()
    
    # Test 2: Three-way AND (Brand AND Color AND Year)
    print("Query: eq(BRAND, 'Toyota') AND eq(COLOR, 'Blue') AND eq(YEAR, 2023)")
    
    def pycq_query_3way():
        return list(collection.retrieve(and_(
            eq(BRAND, "Toyota"),
            eq(COLOR, "Blue"),
            eq(YEAR, 2023)
        )))
    
    def python_query_3way():
        return [c for c in cars if c.brand == "Toyota" and c.color == "Blue" and c.year == 2023]
    
    pycq_time_3way = bench(pycq_query_3way, iterations=100)
    pycq_results_3way = pycq_query_3way()
    
    python_time_3way = bench(python_query_3way, iterations=50)
    python_results_3way = python_query_3way()
    
    speedup_3way = python_time_3way / pycq_time_3way
    
    print(f"Results: {len(pycq_results_3way):,} cars")
    print(f"PyCQEngine:      {pycq_time_3way:>10.2f} μs")
    print(f"Python baseline: {python_time_3way:>10.2f} μs")
    print(f"Speedup:         {speedup_3way:>10.1f}x")
    print()
    
    return {
        'size': n,
        '2way_pycq': pycq_time,
        '2way_python': python_time,
        '2way_speedup': speedup,
        '2way_results': len(pycq_results),
        '3way_pycq': pycq_time_3way,
        '3way_python': python_time_3way,
        '3way_speedup': speedup_3way,
        '3way_results': len(pycq_results_3way),
    }


def main():
    print("=" * 80)
    print("Basic PyCQEngine Benchmark - AND Query Performance")
    print("=" * 80)
    print("\nTesting simple AND queries at different scales\n")
    
    results = []
    
    # Test 1: 10K objects
    results.append(run_test(10_000, "TEST 1"))
    
    # Test 2: 100K objects
    results.append(run_test(100_000, "TEST 2"))
    
    # Test 3: 500K objects
    results.append(run_test(500_000, "TEST 3"))
    
    # Comparison table
    print("=" * 80)
    print("PERFORMANCE COMPARISON")
    print("=" * 80)
    print()
    print("2-Way AND (Brand AND Color):")
    print("-" * 80)
    print(f"{'Size':<12} {'PyCQEngine':<15} {'Python':<15} {'Speedup':<12} {'Results':<10}")
    print("-" * 80)
    for r in results:
        print(f"{r['size']:>10,}  {r['2way_pycq']:>10.2f} μs  {r['2way_python']:>10.2f} μs  {r['2way_speedup']:>8.1f}x  {r['2way_results']:>8,}")
    print()
    
    print("3-Way AND (Brand AND Color AND Year):")
    print("-" * 80)
    print(f"{'Size':<12} {'PyCQEngine':<15} {'Python':<15} {'Speedup':<12} {'Results':<10}")
    print("-" * 80)
    for r in results:
        print(f"{r['size']:>10,}  {r['3way_pycq']:>10.2f} μs  {r['3way_python']:>10.2f} μs  {r['3way_speedup']:>8.1f}x  {r['3way_results']:>8,}")
    print()


if __name__ == "__main__":
    main()
