#!/usr/bin/env python3
"""Fine-grained profiling of AND query to isolate regression."""

import time
import statistics
from pycqengine import IndexedCollection, Attribute, eq, and_


class Car:
    __slots__ = ['vin', 'brand', 'color', 'price', 'year']
    def __init__(self, v, b, c, p, y):
        self.vin = v
        self.brand = b
        self.color = c
        self.price = p
        self.year = y


BRANDS = ["Tesla", "Ford", "BMW", "Toyota", "Honda", "Mercedes", "Audi", "Nissan"]
COLORS = ["Red", "Blue", "Black", "White", "Silver", "Gray"]
YEARS = [2020, 2021, 2022, 2023, 2024]

VIN = Attribute("vin", lambda c: c.vin)
BRAND = Attribute("brand", lambda c: c.brand)
COLOR = Attribute("color", lambda c: c.color)
PRICE = Attribute("price", lambda c: c.price)
YEAR = Attribute("year", lambda c: c.year)


def median_us(times):
    times.sort()
    return statistics.median(times)


def run():
    N = 100_000
    ITERS = 300
    WARMUP = 50

    cars = [
        Car(i, BRANDS[i % 8], COLORS[i % 6], 20000 + (i % 100) * 500, YEARS[i % 5])
        for i in range(N)
    ]
    col = IndexedCollection()
    for a in [VIN, BRAND, COLOR, PRICE, YEAR]:
        col.add_index(a)
    col.add_many(cars)
    mgr = col._manager

    q2 = and_(eq(BRAND, "Tesla"), eq(COLOR, "Red"))

    # Warmup
    for _ in range(WARMUP):
        list(col.retrieve(q2))

    # A: Full pipeline (query + materialize)
    times = []
    for _ in range(ITERS):
        s = time.perf_counter()
        list(col.retrieve(q2))
        times.append((time.perf_counter() - s) * 1e6)
    full_us = median_us(times)

    # B: Rust query_and only (IDs)
    times = []
    for _ in range(ITERS):
        s = time.perf_counter()
        mgr.query_and([("brand", "Tesla"), ("color", "Red")])
        times.append((time.perf_counter() - s) * 1e6)
    ids_us = median_us(times)

    # C: Single eq lookup (brand=Tesla, 12500 results, HashSet clone)
    times = []
    for _ in range(ITERS):
        s = time.perf_counter()
        mgr.query_eq("brand", "Tesla")
        times.append((time.perf_counter() - s) * 1e6)
    eq_us = median_us(times)

    # D: count_eq (zero-alloc, no clone)
    times = []
    for _ in range(ITERS):
        s = time.perf_counter()
        mgr.query_eq_count("brand", "Tesla")
        times.append((time.perf_counter() - s) * 1e6)
    count_us = median_us(times)

    # E: Materialization cost (get_objects)
    ids = mgr.query_and([("brand", "Tesla"), ("color", "Red")])
    cnt = len(ids)
    times = []
    for _ in range(ITERS):
        s = time.perf_counter()
        mgr.get_objects(ids)
        times.append((time.perf_counter() - s) * 1e6)
    mat_us = median_us(times)

    # F: 3-way AND
    times = []
    for _ in range(ITERS):
        s = time.perf_counter()
        mgr.query_and([("brand", "Toyota"), ("color", "Blue"), ("year", 2023)])
        times.append((time.perf_counter() - s) * 1e6)
    and3_us = median_us(times)
    cnt3 = len(mgr.query_and([("brand", "Toyota"), ("color", "Blue"), ("year", 2023)]))

    # G: Two back-to-back query_eq calls (simulates what AND does internally)
    times = []
    for _ in range(ITERS):
        s = time.perf_counter()
        r1 = mgr.query_eq("brand", "Tesla")
        r2 = mgr.query_eq("color", "Red")
        times.append((time.perf_counter() - s) * 1e6)
    two_eq_us = median_us(times)

    brand_cnt = mgr.query_eq_count("brand", "Tesla")
    color_cnt = mgr.query_eq_count("color", "Red")

    print(f"AND 2-way breakdown ({cnt:,} results from {N:,} objects):")
    print(f"  Full (query+materialize):  {full_us:7.1f} us")
    print(f"  IDs only (query_and):      {ids_us:7.1f} us")
    print(f"  Materialize {cnt} objs:       {mat_us:7.1f} us")
    print(f"  Python layer overhead:     {full_us - ids_us - mat_us:7.1f} us")
    print()
    print(f"  Single eq (brand=Tesla):   {eq_us:7.1f} us  ({brand_cnt:,} results, HashSet clone)")
    print(f"  count_eq (zero-alloc):     {count_us:7.1f} us")
    print(f"  Two eq calls back-to-back: {two_eq_us:7.1f} us  (= 2x FFI + 2x clone)")
    print()
    print(f"  query_and overhead vs 2x eq: {ids_us - two_eq_us:+.1f} us  (cache check + intersection + cache store)")
    print()
    print(f"AND 3-way ({cnt3:,} results):")
    print(f"  IDs only (query_and):      {and3_us:7.1f} us")


if __name__ == "__main__":
    run()
