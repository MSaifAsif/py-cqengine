#!/usr/bin/env python3
"""
Lazy ResultSet Performance Benchmark
Tests the impact of eliminating set() conversions and lazy materialization
"""

import time
import statistics
from dataclasses import dataclass
from typing import List
from pycqengine import IndexedCollection, Attribute, eq, and_, or_, in_


@dataclass
class Car:
    vin: int
    brand: str
    model: str
    price: float


# Attributes
VIN = Attribute("vin", lambda c: c.vin)
BRAND = Attribute("brand", lambda c: c.brand)
MODEL = Attribute("model", lambda c: c.model)
PRICE = Attribute("price", lambda c: c.price)


def generate_cars(n: int) -> List[Car]:
    brands = ["Toyota", "Honda", "Ford", "BMW", "Tesla", "Audi", "Mercedes", "Ferrari"]
    cars = []
    for i in range(n):
        cars.append(Car(
            vin=i,
            brand=brands[i % len(brands)],
            model=f"Model_{i}",
            price=20000 + (i % 100) * 1000,
        ))
    return cars


def bench(func, iterations=200):
    """Return median time in μs"""
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        result = func()
        end = time.perf_counter()
        times.append((end - start) * 1_000_000)
    return statistics.median(times), result


def main():
    N = 100_000
    print("=" * 80)
    print("Lazy ResultSet + Pipeline Optimization Benchmark")
    print("=" * 80)
    print(f"\nDataset: {N:,} cars")
    print()

    cars = generate_cars(N)

    collection = IndexedCollection()
    collection.add_index(VIN)
    collection.add_index(BRAND)
    collection.add_index(MODEL)
    collection.add_index(PRICE)
    collection.add_many(cars)
    print(f"✓ Indexed {len(collection):,} cars\n")

    # Python baselines
    def py_count_brand():
        return sum(1 for c in cars if c.brand == "Toyota")

    def py_first_brand():
        for c in cars:
            if c.brand == "Toyota":
                return c
        return None

    def py_first_10_brand():
        result = []
        for c in cars:
            if c.brand == "Toyota":
                result.append(c)
                if len(result) >= 10:
                    break
        return result

    def py_page_brand():
        matches = [c for c in cars if c.brand == "Toyota"]
        return matches[50:100]

    def py_full_list_brand():
        return [c for c in cars if c.brand == "Toyota"]

    # Large result set tests (33% selectivity - "Toyota" = 12,500 results)
    # Adjusted: use 4 brands that together give ~50K results
    def py_count_common():
        return sum(1 for c in cars if c.brand == "Toyota")

    print("=" * 80)
    print("TEST 1: count() — How many results? (NO materialization)")
    print("=" * 80)
    print("Query: eq(BRAND, 'Toyota') → 12,500 results\n")

    t, _ = bench(lambda: collection.retrieve(eq(BRAND, "Toyota")).count())
    py_t, _ = bench(py_count_brand)
    print(f"  PyCQEngine .count():  {t:>10.2f} μs  (no materialization!)")
    print(f"  Python sum(gen):       {py_t:>10.2f} μs  (scans all {N:,})")
    print(f"  Speedup:              {py_t/t:>10.1f}x ⚡\n")

    print("=" * 80)
    print("TEST 2: first(1) — Get first result only")
    print("=" * 80)
    print("Query: eq(BRAND, 'Toyota') → materialize 1 of 12,500\n")

    t, r = bench(lambda: collection.retrieve(eq(BRAND, "Toyota")).first(1))
    py_t, _ = bench(py_first_brand)
    print(f"  PyCQEngine .first(1): {t:>10.2f} μs  (1 object materialized)")
    print(f"  Python early-exit:     {py_t:>10.2f} μs  (scans until found)")
    print(f"  Speedup:              {py_t/t:>10.1f}x ⚡\n")

    print("=" * 80)
    print("TEST 3: first(10) — Get first 10 results")
    print("=" * 80)
    print("Query: eq(BRAND, 'Toyota') → materialize 10 of 12,500\n")

    t, r = bench(lambda: collection.retrieve(eq(BRAND, "Toyota")).first(10))
    py_t, _ = bench(py_first_10_brand)
    print(f"  PyCQEngine .first(10): {t:>10.2f} μs  (10 objects materialized)")
    print(f"  Python early-exit:      {py_t:>10.2f} μs  (scans + collects 10)")
    print(f"  Speedup:               {py_t/t:>10.1f}x ⚡\n")

    print("=" * 80)
    print("TEST 4: slice(50,100) — Pagination (page 2)")
    print("=" * 80)
    print("Query: eq(BRAND, 'Toyota') → materialize 50 of 12,500\n")

    t, r = bench(lambda: collection.retrieve(eq(BRAND, "Toyota")).slice(50, 100))
    py_t, _ = bench(py_page_brand)
    print(f"  PyCQEngine .slice():  {t:>10.2f} μs  (50 objects materialized)")
    print(f"  Python list + slice:   {py_t:>10.2f} μs  (materializes ALL then slices)")
    print(f"  Speedup:              {py_t/t:>10.1f}x ⚡\n")

    print("=" * 80)
    print("TEST 5: list(results) — Full materialization comparison")
    print("=" * 80)
    print("Query: eq(BRAND, 'Toyota') → ALL 12,500 results\n")

    t, r = bench(lambda: list(collection.retrieve(eq(BRAND, "Toyota"))))
    py_t, _ = bench(py_full_list_brand)
    print(f"  PyCQEngine list():    {t:>10.2f} μs  ({len(r):,} objects)")
    print(f"  Python list comp:      {py_t:>10.2f} μs  ({len(r):,} objects)")
    print(f"  Speedup:              {py_t/t:>10.1f}x\n")

    print("=" * 80)
    print("TEST 6: Point lookup — eq(VIN, 50000)")
    print("=" * 80)
    print("Query: eq(VIN, 50000) → 1 result\n")

    t, _ = bench(lambda: list(collection.retrieve(eq(VIN, 50000))))
    py_t, _ = bench(lambda: [c for c in cars if c.vin == 50000])
    print(f"  PyCQEngine:           {t:>10.2f} μs")
    print(f"  Python list comp:      {py_t:>10.2f} μs")
    print(f"  Speedup:              {py_t/t:>10.1f}x ⚡\n")

    print("=" * 80)
    print("TEST 7: Rare value — eq(BRAND, 'Ferrari')")
    print("=" * 80)
    print("Query: eq(BRAND, 'Ferrari') → ~133 results\n")

    t, r = bench(lambda: list(collection.retrieve(eq(BRAND, "Ferrari"))))
    py_t, _ = bench(lambda: [c for c in cars if c.brand == "Ferrari"])
    print(f"  PyCQEngine:           {t:>10.2f} μs  ({len(r):,} results)")
    print(f"  Python list comp:      {py_t:>10.2f} μs")
    print(f"  Speedup:              {py_t/t:>10.1f}x ⚡\n")

    print("=" * 80)
    print("TEST 8: Large result set — 33% selectivity (THE critical test)")
    print("=" * 80)

    # Create a dataset with 33% of one brand
    special_cars = []
    for i in range(N):
        if i < N // 3:
            brand = "CommonBrand"
        else:
            brands_other = ["A", "B", "C", "D", "E", "F", "G"]
            brand = brands_other[i % len(brands_other)]
        special_cars.append(Car(vin=N + i, brand=brand, model=f"Sp_{i}", price=30000))

    coll2 = IndexedCollection()
    coll2.add_index(Attribute("vin", lambda c: c.vin))
    coll2.add_index(Attribute("brand", lambda c: c.brand))
    coll2.add_many(special_cars)

    result_count = coll2.retrieve(eq(Attribute("brand", lambda c: c.brand), "CommonBrand")).count()
    print(f"Query: eq(BRAND, 'CommonBrand') → {result_count:,} results ({result_count*100/N:.1f}%)\n")

    BRAND2 = Attribute("brand", lambda c: c.brand)

    # count only
    t_count, _ = bench(lambda: coll2.retrieve(eq(BRAND2, "CommonBrand")).count())
    py_count_t, _ = bench(lambda: sum(1 for c in special_cars if c.brand == "CommonBrand"))
    print(f"  .count() only:        {t_count:>10.2f} μs  vs Python {py_count_t:>10.2f} μs  → {py_count_t/t_count:.1f}x ⚡")

    # first(10) only
    t_first, _ = bench(lambda: coll2.retrieve(eq(BRAND2, "CommonBrand")).first(10))
    py_first_t, _ = bench(lambda: [c for c in special_cars if c.brand == "CommonBrand"][:10])
    print(f"  .first(10):           {t_first:>10.2f} μs  vs Python {py_first_t:>10.2f} μs  → {py_first_t/t_first:.1f}x ⚡")

    # slice(0, 50)
    t_slice, _ = bench(lambda: coll2.retrieve(eq(BRAND2, "CommonBrand")).slice(0, 50))
    py_slice_t, _ = bench(lambda: [c for c in special_cars if c.brand == "CommonBrand"][:50])
    print(f"  .slice(0,50):         {t_slice:>10.2f} μs  vs Python {py_slice_t:>10.2f} μs  → {py_slice_t/t_slice:.1f}x ⚡")

    # full list() — THE bottleneck test
    t_full, r_full = bench(lambda: list(coll2.retrieve(eq(BRAND2, "CommonBrand"))))
    py_full_t, r_py = bench(lambda: [c for c in special_cars if c.brand == "CommonBrand"])
    print(f"  list() full {len(r_full):,}:   {t_full:>10.2f} μs  vs Python {py_full_t:>10.2f} μs  → {py_full_t/t_full:.1f}x")

    print()
    print("=" * 80)
    print("TEST 9: AND Query Performance (with pipeline optimization)")
    print("=" * 80)
    print("Query: eq(BRAND, 'Toyota') AND eq(PRICE, 20000)\n")

    t, r = bench(lambda: list(collection.retrieve(and_(eq(BRAND, "Toyota"), eq(PRICE, 20000)))))
    py_t, _ = bench(lambda: [c for c in cars if c.brand == "Toyota" and c.price == 20000])
    print(f"  PyCQEngine:           {t:>10.2f} μs  ({len(r):,} results)")
    print(f"  Python:                {py_t:>10.2f} μs")
    print(f"  Speedup:              {py_t/t:>10.1f}x\n")

    print("=" * 80)
    print("TEST 10: results[0] — Single index access")
    print("=" * 80)
    print("Query: eq(BRAND, 'Toyota')[0] → 1 object\n")

    t, _ = bench(lambda: collection.retrieve(eq(BRAND, "Toyota"))[0])
    py_t, _ = bench(py_first_brand)
    print(f"  PyCQEngine [0]:       {t:>10.2f} μs  (single object)")
    print(f"  Python early-exit:     {py_t:>10.2f} μs")
    print(f"  Speedup:              {py_t/t:>10.1f}x ⚡\n")


if __name__ == "__main__":
    main()
