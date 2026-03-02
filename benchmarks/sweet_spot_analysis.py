"""
PyCQEngine Sweet Spot Analysis

This benchmark compares performance across different result set sizes
to identify the "sweet spot" where indexed queries provide maximum benefit.

Hypothesis: PyCQEngine excels at selective queries (small result sets)
"""

import time
import random
from typing import List

from pycqengine import IndexedCollection, Attribute, eq


class Car:
    """Lightweight car object"""
    __slots__ = ['vin', 'brand', 'model', 'price']
    
    def __init__(self, vin: int, brand: str, model: str, price: int):
        self.vin = vin
        self.brand = brand
        self.model = model
        self.price = price


def measure_query(collection, cars_list, query_attr, query_value, label: str):
    """Measure both indexed and non-indexed query performance"""
    
    # Indexed query (PyCQEngine)
    times_indexed = []
    for _ in range(100):
        start = time.perf_counter_ns()
        results_indexed = list(collection.retrieve(eq(query_attr, query_value)))
        elapsed = (time.perf_counter_ns() - start) / 1000  # microseconds
        times_indexed.append(elapsed)
    
    indexed_median = sorted(times_indexed)[50]
    result_count = len(results_indexed)
    
    # Python list comprehension baseline
    times_python = []
    for _ in range(100):
        start = time.perf_counter_ns()
        if query_attr.name == 'vin':
            results_python = [c for c in cars_list if c.vin == query_value]
        elif query_attr.name == 'brand':
            results_python = [c for c in cars_list if c.brand == query_value]
        elif query_attr.name == 'model':
            results_python = [c for c in cars_list if c.model == query_value]
        elapsed = (time.perf_counter_ns() - start) / 1000
        times_python.append(elapsed)
    
    python_median = sorted(times_python)[50]
    speedup = python_median / indexed_median
    
    # Calculate selectivity
    selectivity = (result_count / len(cars_list)) * 100
    
    print(f"\n{label}")
    print(f"  Results:          {result_count:>8,} ({selectivity:.3f}% of dataset)")
    print(f"  PyCQEngine:       {indexed_median:>8.2f} μs")
    print(f"  List Comp:        {python_median:>8.2f} μs")
    print(f"  Speedup:          {speedup:>8.1f}x faster")
    
    if speedup > 1:
        print(f"  Time saved:       {python_median - indexed_median:>8.2f} μs")
    
    return {
        'label': label,
        'results': result_count,
        'selectivity': selectivity,
        'indexed': indexed_median,
        'python': python_median,
        'speedup': speedup
    }


def main():
    N = 100_000
    
    print("=" * 80)
    print("PyCQEngine Sweet Spot Analysis")
    print("=" * 80)
    print(f"\nDataset: {N:,} car objects")
    print("Testing queries with varying selectivity...\n")
    
    # Generate data with controlled distribution
    random.seed(42)
    
    # Create brands with different frequencies
    brands_common = ["Toyota", "Ford", "Honda"]  # ~40% each → 12.5K results
    brands_rare = ["Ferrari", "Lamborghini", "Bugatti"]  # ~0.05% each → 50 results
    
    # Create unique models (one per car for point lookups)
    models = [f"Model_{i}" for i in range(N)]
    
    print("Generating test data...")
    cars_list = []
    for i in range(N):
        # 80% common brands, 20% rare brands
        if i < N * 0.8:
            brand = random.choice(brands_common)
        else:
            # Sprinkle rare cars throughout
            brand = random.choice(brands_rare) if random.random() < 0.02 else random.choice(brands_common)
        
        model = models[i]
        price = random.randint(20000, 100000)
        cars_list.append(Car(i, brand, model, price))
    
    print(f"✓ Generated {N:,} cars")
    
    # Setup indexed collection
    print("\nBuilding indexed collection...")
    start = time.perf_counter()
    
    collection = IndexedCollection()
    VIN = Attribute("vin", lambda c: c.vin)
    BRAND = Attribute("brand", lambda c: c.brand)
    MODEL = Attribute("model", lambda c: c.model)
    
    collection.add_index(VIN)
    collection.add_index(BRAND)
    collection.add_index(MODEL)
    
    collection.add_many(cars_list)
    
    build_time = time.perf_counter() - start
    print(f"✓ Indexed {N:,} cars in {build_time:.2f}s")
    
    # ========================================================================
    # TEST DIFFERENT SELECTIVITY LEVELS
    # ========================================================================
    print("\n" + "=" * 80)
    print("QUERY PERFORMANCE BY SELECTIVITY")
    print("=" * 80)
    
    results = []
    
    # Test 1: Point lookup (1 result, 0.001% selectivity)
    results.append(measure_query(
        collection, cars_list, VIN, 50000,
        "📍 Point Lookup (Unique VIN)"
    ))
    
    # Test 2: Very selective (model lookup, 1 result)
    results.append(measure_query(
        collection, cars_list, MODEL, "Model_50000",
        "🎯 Very Selective (Unique Model)"
    ))
    
    # Test 3: Highly selective (~50 results, 0.05%)
    results.append(measure_query(
        collection, cars_list, BRAND, "Ferrari",
        "⭐ Highly Selective (Rare Brand)"
    ))
    
    # Test 4: Moderate selectivity (~12,500 results, ~12.5%)
    results.append(measure_query(
        collection, cars_list, BRAND, "Toyota",
        "📊 Moderate Selectivity (Common Brand)"
    ))
    
    # ========================================================================
    # ANALYSIS
    # ========================================================================
    print("\n" + "=" * 80)
    print("ANALYSIS: When Does PyCQEngine Win?")
    print("=" * 80)
    
    print("\n📈 Speedup by Selectivity:")
    for r in results:
        bar_length = int(r['speedup'])
        bar = "█" * bar_length
        print(f"  {r['selectivity']:>6.3f}%  {r['speedup']:>6.1f}x  {bar}")
    
    print("\n💡 Key Insights:")
    
    # Find best case
    best = max(results, key=lambda x: x['speedup'])
    print(f"\n  1. BEST CASE: {best['label']}")
    print(f"     - Speedup: {best['speedup']:.1f}x faster")
    print(f"     - Query time: {best['indexed']:.2f} μs vs {best['python']:.2f} μs")
    print(f"     - Sweet spot: {best['results']:,} result(s)")
    
    # Find break-even point
    print(f"\n  2. PERFORMANCE PATTERN:")
    for i, r in enumerate(results):
        if r['speedup'] >= 10:
            status = "🔥 EXCELLENT"
        elif r['speedup'] >= 3:
            status = "✅ GOOD"
        elif r['speedup'] >= 1.5:
            status = "⚠️  MARGINAL"
        else:
            status = "❌ NOT WORTH IT"
        
        print(f"     {r['results']:>8,} results ({r['selectivity']:>6.3f}%) → {r['speedup']:>5.1f}x  {status}")
    
    print(f"\n  3. BOTTLENECK ANALYSIS:")
    
    # Estimate costs
    for r in results:
        lookup_cost = 50  # μs (estimated, relatively constant)
        materialize_cost = r['results'] * 0.15  # μs per object
        total_estimated = lookup_cost + materialize_cost
        
        print(f"\n     {r['label']}:")
        print(f"       Measured:     {r['indexed']:>8.2f} μs")
        print(f"       Index lookup: ~{lookup_cost:>7.2f} μs (estimated)")
        print(f"       Materialize:  ~{materialize_cost:>7.2f} μs ({r['results']:,} objects)")
    

if __name__ == "__main__":
    main()
