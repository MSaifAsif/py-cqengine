"""
PyCQEngine Detailed Performance Profiling

This benchmark breaks down performance by individual operation to identify:
1. Where we spend the most time
2. Where we get the biggest performance wins
3. Potential bottlenecks in the pipeline

Test size: 100,000 objects (fast iteration)
"""

import time
import random
from typing import List

from pycqengine import IndexedCollection, Attribute, eq


class Car:
    """Lightweight car object for testing"""
    __slots__ = ['vin', 'brand', 'price']
    
    def __init__(self, vin: int, brand: str, price: int):
        self.vin = vin
        self.brand = brand
        self.price = price


def timer(name: str):
    """Context manager for precise timing"""
    class TimerContext:
        def __enter__(self):
            self.start = time.perf_counter_ns()
            return self
        
        def __exit__(self, *args):
            self.end = time.perf_counter_ns()
            self.elapsed_ns = self.end - self.start
            self.elapsed_us = self.elapsed_ns / 1000
            self.elapsed_ms = self.elapsed_us / 1000
            print(f"  {name:.<50} {self.elapsed_ms:>10.2f} ms  ({self.elapsed_us:>12.2f} μs)")
    
    return TimerContext()


def generate_test_data(n: int) -> List[Car]:
    """Generate test dataset"""
    random.seed(42)
    brands = ["Tesla", "Ford", "BMW", "Toyota", "Honda", "Mercedes", "Audi", "Nissan"]
    
    cars = []
    for i in range(n):
        brand = random.choice(brands)
        price = random.randint(20000, 100000)
        cars.append(Car(i, brand, price))
    
    return cars


def main():
    N = 100_000
    
    print("=" * 80)
    print("PyCQEngine Detailed Performance Profile")
    print("=" * 80)
    print(f"\nDataset: {N:,} car objects")
    print(f"Query: eq(BRAND, 'Tesla') - Expected ~{N//8:,} results\n")
    
    # ========================================================================
    # PHASE 1: DATA PREPARATION
    # ========================================================================
    print("=" * 80)
    print("PHASE 1: Data Preparation")
    print("=" * 80)
    
    with timer("Generate Python objects") as t:
        cars_list = generate_test_data(N)
    
    memory_per_obj = 64  # Approximate for __slots__
    print(f"\n  Memory estimate: {N * memory_per_obj / 1_000_000:.1f} MB")
    print(f"  Throughput: {N / (t.elapsed_ms / 1000):,.0f} objects/sec")
    
    # ========================================================================
    # PHASE 2: INDEX SETUP
    # ========================================================================
    print("\n" + "=" * 80)
    print("PHASE 2: Index Setup")
    print("=" * 80)
    
    with timer("Create IndexedCollection") as t:
        collection = IndexedCollection()
    
    # Define attributes
    VIN = Attribute("vin", lambda c: c.vin)
    BRAND = Attribute("brand", lambda c: c.brand)
    PRICE = Attribute("price", lambda c: c.price)
    
    with timer("Register VIN attribute") as t:
        collection.add_index(VIN)
    
    with timer("Register BRAND attribute") as t:
        collection.add_index(BRAND)
    
    with timer("Register PRICE attribute") as t:
        collection.add_index(PRICE)
    
    # ========================================================================
    # PHASE 3: DATA INSERTION (Batch)
    # ========================================================================
    print("\n" + "=" * 80)
    print("PHASE 3: Batch Data Insertion")
    print("=" * 80)
    
    with timer("Extract attributes (Python side)") as t_extract:
        # Simulate what happens inside add_many
        extracted_data = []
        for car in cars_list:
            attributes = {
                'vin': VIN.extract(car),
                'brand': BRAND.extract(car),
                'price': PRICE.extract(car),
            }
            extracted_data.append((car, attributes))
    
    print(f"  Per-object cost: {t_extract.elapsed_us / N:.3f} μs")
    
    with timer("Send batch to Rust (FFI crossing)") as t_ffi:
        collection.add_many(cars_list)
    
    print(f"  Total objects inserted: {len(collection):,}")
    print(f"  Throughput: {N / (t_ffi.elapsed_ms / 1000):,.0f} objects/sec")
    print(f"  Per-object insertion: {t_ffi.elapsed_us / N:.3f} μs")
    
    # Breakdown: Python extraction vs Rust indexing
    rust_time = t_ffi.elapsed_ms - t_extract.elapsed_ms
    print(f"\n  Time breakdown:")
    print(f"    Python attribute extraction: {t_extract.elapsed_ms:>8.2f} ms ({t_extract.elapsed_ms/t_ffi.elapsed_ms*100:.1f}%)")
    print(f"    Rust indexing + FFI:         {rust_time:>8.2f} ms ({rust_time/t_ffi.elapsed_ms*100:.1f}%)")
    
    # ========================================================================
    # PHASE 4: QUERY EXECUTION (Detailed Breakdown)
    # ========================================================================
    print("\n" + "=" * 80)
    print("PHASE 4: Query Execution Breakdown")
    print("=" * 80)
    print("\nQuery: eq(BRAND, 'Tesla')\n")
    
    # Warmup
    for _ in range(10):
        _ = collection.retrieve(eq(BRAND, "Tesla"))
    
    # Part A: Query construction (Python side)
    with timer("Build query object (Python)") as t:
        query = eq(BRAND, "Tesla")
    
    # Part B: Query execution (Rust index lookup)
    with timer("Execute query (Rust index lookup)") as t_query:
        result_set = collection.retrieve(query)
    
    result_count = len(result_set)
    print(f"  Results found: {result_count:,}")
    
    # Part C: Get result count (minimal overhead)
    with timer("Get result count len(result_set)") as t:
        count = len(result_set)
    
    print(f"  ✓ Index lookup cost: {t_query.elapsed_us:.2f} μs for {result_count:,} IDs")
    
    # Part D: Materialization (converting IDs to Python objects)
    with timer("Materialize results list(result_set)") as t_materialize:
        results = list(result_set)
    
    print(f"  Per-object retrieval: {t_materialize.elapsed_us / result_count:.3f} μs")
    
    # Total query time
    total_query_time = t_query.elapsed_us + t_materialize.elapsed_us
    print(f"\n  Total query time: {total_query_time:.2f} μs ({total_query_time/1000:.2f} ms)")
    print(f"    Index lookup:     {t_query.elapsed_us:>10.2f} μs ({t_query.elapsed_us/total_query_time*100:.1f}%)")
    print(f"    Materialization:  {t_materialize.elapsed_us:>10.2f} μs ({t_materialize.elapsed_us/total_query_time*100:.1f}%)")
    
    # ========================================================================
    # PHASE 5: BASELINE COMPARISON (Pure Python)
    # ========================================================================
    print("\n" + "=" * 80)
    print("PHASE 5: Baseline Comparison (Pure Python)")
    print("=" * 80)
    
    # Pure Python list comprehension
    with timer("List comprehension [x for x in ...]") as t_listcomp:
        python_results = [c for c in cars_list if c.brand == "Tesla"]
    
    print(f"  Results found: {len(python_results):,}")
    print(f"  Per-object scan: {t_listcomp.elapsed_us / N:.3f} μs")
    
    # Pure Python filter (similar but with iterator)
    with timer("Filter + list conversion") as t_filter:
        python_results2 = list(filter(lambda c: c.brand == "Tesla", cars_list))
    
    # Count-only (no materialization)
    with timer("Count-only (sum generator)") as t_count:
        count = sum(1 for c in cars_list if c.brand == "Tesla")
    
    # ========================================================================
    # PHASE 6: PERFORMANCE ANALYSIS
    # ========================================================================
    print("\n" + "=" * 80)
    print("PHASE 6: Performance Analysis & Wins")
    print("=" * 80)
    
    # Query execution speedup
    speedup_query = t_listcomp.elapsed_us / t_query.elapsed_us
    speedup_total = t_listcomp.elapsed_us / total_query_time
    
    print(f"\n🏆 SPEEDUP vs List Comprehension:")
    print(f"  Index lookup only:  {speedup_query:>8.1f}x faster")
    print(f"  Full query (w/ mat): {speedup_total:>8.1f}x faster")
    
    print(f"\n📊 TIME DISTRIBUTION:")
    total_pipeline = t_ffi.elapsed_ms + total_query_time/1000
    print(f"  Data insertion:     {t_ffi.elapsed_ms:>10.2f} ms ({t_ffi.elapsed_ms/total_pipeline*100:.1f}%)")
    print(f"  Query execution:    {total_query_time/1000:>10.2f} ms ({total_query_time/1000/total_pipeline*100:.1f}%)")
    
    print(f"\n💰 BIGGEST WINS:")
    print(f"  1. Index lookup:      {speedup_query:.1f}x faster than full scan")
    print(f"     - Rust hash lookup:  O(1) vs Python O(n) scan")
    print(f"     - Time saved:        {t_listcomp.elapsed_ms - t_query.elapsed_ms:.2f} ms")
    
    print(f"\n  2. Batch insertion:   {N / (t_ffi.elapsed_ms / 1000):,.0f} objects/sec")
    print(f"     - Amortized FFI cost: {t_ffi.elapsed_us / N:.3f} μs per object")
    
    print(f"\n⚠️  BOTTLENECKS:")
    if t_materialize.elapsed_us > t_query.elapsed_us * 10:
        print(f"  - Materialization is {t_materialize.elapsed_us/t_query.elapsed_us:.1f}x slower than lookup")
        print(f"    ({t_materialize.elapsed_us:.2f} μs to convert {result_count:,} IDs → Python objects)")
    
    print(f"\n✨ KEY INSIGHTS:")
    print(f"  • Index lookup cost:        {t_query.elapsed_us / result_count:.4f} μs per result")
    print(f"  • Materialization cost:     {t_materialize.elapsed_us / result_count:.4f} μs per object")
    print(f"  • Python full scan cost:    {t_listcomp.elapsed_us / N:.4f} μs per object")
    print(f"  • Breakeven point:          ~{int(t_query.elapsed_us / (t_listcomp.elapsed_us / N)):,} objects")
    print(f"    (indexed query faster when result set < this size)")
    
    # ========================================================================
    # PHASE 7: REPEATED QUERIES (Cache behavior)
    # ========================================================================
    print("\n" + "=" * 80)
    print("PHASE 7: Repeated Query Performance")
    print("=" * 80)
    
    iterations = 1000
    
    with timer(f"Run query {iterations}x (query only)") as t:
        for _ in range(iterations):
            _ = collection.retrieve(eq(BRAND, "Tesla"))
    
    avg_query_us = t.elapsed_us / iterations
    print(f"  Average query time: {avg_query_us:.2f} μs")
    print(f"  Throughput: {iterations / (t.elapsed_ms / 1000):,.0f} queries/sec")
    
    with timer(f"Run query {iterations}x (with materialization)") as t:
        for _ in range(iterations):
            _ = list(collection.retrieve(eq(BRAND, "Tesla")))
    
    avg_total_us = t.elapsed_us / iterations
    print(f"  Average total time: {avg_total_us:.2f} μs")
    print(f"  Throughput: {iterations / (t.elapsed_ms / 1000):,.0f} full queries/sec")


if __name__ == "__main__":
    main()
