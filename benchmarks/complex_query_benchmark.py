#!/usr/bin/env python3
"""
Benchmark for Complex Queries (AND/OR/IN) Performance
Tests the optimization of set operations moved to Rust
"""

import time
import statistics
from dataclasses import dataclass
from typing import List
from pycqengine import IndexedCollection, Attribute, eq, and_, or_, in_


@dataclass
class Product:
    """Product with multiple filterable attributes"""
    id: int
    category: str
    brand: str
    price_range: str
    in_stock: bool
    rating: int


# Attributes
ID = Attribute("id", lambda p: p.id)
CATEGORY = Attribute("category", lambda p: p.category)
BRAND = Attribute("brand", lambda p: p.brand)
PRICE_RANGE = Attribute("price_range", lambda p: p.price_range)
IN_STOCK = Attribute("in_stock", lambda p: p.in_stock)
RATING = Attribute("rating", lambda p: p.rating)


def generate_products(n: int) -> List[Product]:
    """Generate test product dataset"""
    categories = ["Electronics", "Clothing", "Books", "Home", "Sports"]
    brands = ["BrandA", "BrandB", "BrandC", "BrandD", "BrandE"]
    price_ranges = ["$", "$$", "$$$", "$$$$"]
    ratings = [1, 2, 3, 4, 5]
    
    products = []
    for i in range(n):
        products.append(Product(
            id=i,
            category=categories[i % len(categories)],
            brand=brands[i % len(brands)],
            price_range=price_ranges[i % len(price_ranges)],
            in_stock=(i % 3) != 0,  # ~67% in stock
            rating=ratings[i % len(ratings)]
        ))
    
    return products


def benchmark(func, iterations=100):
    """Run benchmark and return median time in microseconds"""
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        result = func()
        end = time.perf_counter()
        times.append((end - start) * 1_000_000)  # Convert to microseconds
    
    return statistics.median(times), len(result) if hasattr(result, '__len__') else 0


def main():
    print("=" * 80)
    print("Complex Query Performance Benchmark")
    print("=" * 80)
    print()
    
    # Setup
    n_products = 100_000
    print(f"Dataset: {n_products:,} products")
    print()
    
    print("Generating test data...")
    products = generate_products(n_products)
    print(f"✓ Generated {len(products):,} products")
    print()
    
    # Build indexed collection
    print("Building indexed collection...")
    collection = IndexedCollection()
    collection.add_index(ID)
    collection.add_index(CATEGORY)
    collection.add_index(BRAND)
    collection.add_index(PRICE_RANGE)
    collection.add_index(IN_STOCK)
    collection.add_index(RATING)
    
    start = time.time()
    collection.add_many(products)
    elapsed = time.time() - start
    print(f"✓ Indexed {len(collection):,} products in {elapsed:.2f}s")
    print()
    
    # Python baseline implementations
    def python_and_2():
        """Python AND with 2 conditions"""
        return [p for p in products 
                if p.category == "Electronics" and p.in_stock]
    
    def python_and_3():
        """Python AND with 3 conditions"""
        return [p for p in products 
                if p.category == "Electronics" and p.brand == "BrandA" and p.in_stock]
    
    def python_or_2():
        """Python OR with 2 conditions"""
        return [p for p in products 
                if p.brand == "BrandA" or p.brand == "BrandB"]
    
    def python_or_3():
        """Python OR with 3 conditions"""
        return [p for p in products 
                if p.brand == "BrandA" or p.brand == "BrandB" or p.brand == "BrandC"]
    
    def python_in():
        """Python IN query"""
        brands = {"BrandA", "BrandB", "BrandC"}
        return [p for p in products if p.brand in brands]
    
    print("=" * 80)
    print("TEST 1: AND Query with 2 Conditions")
    print("=" * 80)
    print()
    print("Query: category='Electronics' AND in_stock=True")
    print()
    
    # PyCQEngine
    pycq_time, pycq_count = benchmark(
        lambda: list(collection.retrieve(and_(
            eq(CATEGORY, "Electronics"),
            eq(IN_STOCK, True)
        )))
    )
    
    # Python
    py_time, py_count = benchmark(python_and_2)
    
    speedup = py_time / pycq_time
    print(f"PyCQEngine:     {pycq_time:>10.2f} μs  ({pycq_count:>6,} results)")
    print(f"Python baseline: {py_time:>10.2f} μs  ({py_count:>6,} results)")
    print(f"Speedup:        {speedup:>10.1f}x {'⚡' * min(5, int(speedup)//10)}")
    print()
    
    print("=" * 80)
    print("TEST 2: AND Query with 3 Conditions")
    print("=" * 80)
    print()
    print("Query: category='Electronics' AND brand='BrandA' AND in_stock=True")
    print()
    
    # PyCQEngine
    pycq_time, pycq_count = benchmark(
        lambda: list(collection.retrieve(and_(
            eq(CATEGORY, "Electronics"),
            eq(BRAND, "BrandA"),
            eq(IN_STOCK, True)
        )))
    )
    
    # Python
    py_time, py_count = benchmark(python_and_3)
    
    speedup = py_time / pycq_time
    print(f"PyCQEngine:     {pycq_time:>10.2f} μs  ({pycq_count:>6,} results)")
    print(f"Python baseline: {py_time:>10.2f} μs  ({py_count:>6,} results)")
    print(f"Speedup:        {speedup:>10.1f}x {'⚡' * min(5, int(speedup)//10)}")
    print()
    
    print("=" * 80)
    print("TEST 3: OR Query with 2 Conditions")
    print("=" * 80)
    print()
    print("Query: brand='BrandA' OR brand='BrandB'")
    print()
    
    # PyCQEngine
    pycq_time, pycq_count = benchmark(
        lambda: list(collection.retrieve(or_(
            eq(BRAND, "BrandA"),
            eq(BRAND, "BrandB")
        )))
    )
    
    # Python
    py_time, py_count = benchmark(python_or_2)
    
    speedup = py_time / pycq_time
    print(f"PyCQEngine:     {pycq_time:>10.2f} μs  ({pycq_count:>6,} results)")
    print(f"Python baseline: {py_time:>10.2f} μs  ({py_count:>6,} results)")
    print(f"Speedup:        {speedup:>10.1f}x {'⚡' * min(5, int(speedup)//10)}")
    print()
    
    print("=" * 80)
    print("TEST 4: OR Query with 3 Conditions")
    print("=" * 80)
    print()
    print("Query: brand='BrandA' OR brand='BrandB' OR brand='BrandC'")
    print()
    
    # PyCQEngine
    pycq_time, pycq_count = benchmark(
        lambda: list(collection.retrieve(or_(
            eq(BRAND, "BrandA"),
            eq(BRAND, "BrandB"),
            eq(BRAND, "BrandC")
        )))
    )
    
    # Python
    py_time, py_count = benchmark(python_or_3)
    
    speedup = py_time / pycq_time
    print(f"PyCQEngine:     {pycq_time:>10.2f} μs  ({pycq_count:>6,} results)")
    print(f"Python baseline: {py_time:>10.2f} μs  ({py_count:>6,} results)")
    print(f"Speedup:        {speedup:>10.1f}x {'⚡' * min(5, int(speedup)//10)}")
    print()
    
    print("=" * 80)
    print("TEST 5: IN Query")
    print("=" * 80)
    print()
    print("Query: brand IN ['BrandA', 'BrandB', 'BrandC']")
    print()
    
    # PyCQEngine
    pycq_time, pycq_count = benchmark(
        lambda: list(collection.retrieve(in_(BRAND, ["BrandA", "BrandB", "BrandC"])))
    )
    
    # Python
    py_time, py_count = benchmark(python_in)
    
    speedup = py_time / pycq_time
    print(f"PyCQEngine:     {pycq_time:>10.2f} μs  ({pycq_count:>6,} results)")
    print(f"Python baseline: {py_time:>10.2f} μs  ({py_count:>6,} results)")
    print(f"Speedup:        {speedup:>10.1f}x {'⚡' * min(5, int(speedup)//10)}")
    print()
    
    print("=" * 80)
    print("TEST 6: Complex AND Query (4 Conditions)")
    print("=" * 80)
    print()
    print("Query: category='Electronics' AND price_range='$$' AND in_stock=True AND rating>=4")
    print()
    
    # PyCQEngine
    pycq_time, pycq_count = benchmark(
        lambda: list(collection.retrieve(and_(
            eq(CATEGORY, "Electronics"),
            eq(PRICE_RANGE, "$$"),
            eq(IN_STOCK, True),
            eq(RATING, 4)
        )))
    )
    
    # Python
    def python_and_4():
        return [p for p in products 
                if p.category == "Electronics" 
                and p.price_range == "$$" 
                and p.in_stock 
                and p.rating == 4]
    
    py_time, py_count = benchmark(python_and_4)
    
    speedup = py_time / pycq_time
    print(f"PyCQEngine:     {pycq_time:>10.2f} μs  ({pycq_count:>6,} results)")
    print(f"Python baseline: {py_time:>10.2f} μs  ({py_count:>6,} results)")
    print(f"Speedup:        {speedup:>10.1f}x {'⚡' * min(5, int(speedup)//10)}")
    print()


if __name__ == "__main__":
    main()
