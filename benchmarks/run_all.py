#!/usr/bin/env python3
"""
PyCQEngine Unified Benchmark Suite
===================================
Single script that runs all key performance scenarios and outputs
a compact summary table + optional JSON for tracking across iterations.

Usage:
    python benchmarks/run_all.py              # Run all benchmarks
    python benchmarks/run_all.py --json       # Also output JSON
    python benchmarks/run_all.py --quick      # Quick mode (fewer iterations)
"""

import argparse
import json
import sys
import time
import statistics
from typing import List, Dict, Any

from pycqengine import IndexedCollection, Attribute, eq, and_, or_, in_, gt, gte, lt, lte, between


# ---------------------------------------------------------------------------
# Test data
# ---------------------------------------------------------------------------

class Car:
    __slots__ = ['vin', 'brand', 'color', 'price', 'year']
    def __init__(self, vin: int, brand: str, color: str, price: int, year: int):
        self.vin = vin
        self.brand = brand
        self.color = color
        self.price = price
        self.year = year


BRANDS = ["Tesla", "Ford", "BMW", "Toyota", "Honda", "Mercedes", "Audi", "Nissan"]
COLORS = ["Red", "Blue", "Black", "White", "Silver", "Gray"]
YEARS  = [2020, 2021, 2022, 2023, 2024]

VIN   = Attribute("vin",   lambda c: c.vin)
BRAND = Attribute("brand", lambda c: c.brand)
COLOR = Attribute("color", lambda c: c.color)
PRICE = Attribute("price", lambda c: c.price)
YEAR  = Attribute("year",  lambda c: c.year)


def generate_cars(n: int) -> List[Car]:
    return [
        Car(i, BRANDS[i % 8], COLORS[i % 6], 20000 + (i % 100) * 500, YEARS[i % 5])
        for i in range(n)
    ]


# ---------------------------------------------------------------------------
# Timing helpers
# ---------------------------------------------------------------------------

def bench(func, iterations: int = 100, warmup: int = 10) -> Dict[str, float]:
    """Return median, p5, p95 in microseconds."""
    for _ in range(warmup):
        func()
    times = []
    for _ in range(iterations):
        s = time.perf_counter()
        func()
        times.append((time.perf_counter() - s) * 1e6)
    times.sort()
    return {
        "median": statistics.median(times),
        "p5":     times[max(0, len(times) * 5 // 100)],
        "p95":    times[min(len(times) - 1, len(times) * 95 // 100)],
    }


def bench_python(func, iterations: int = 50, warmup: int = 5) -> float:
    """Return median time for Python baseline (fewer iterations)."""
    for _ in range(warmup):
        func()
    times = []
    for _ in range(iterations):
        s = time.perf_counter()
        func()
        times.append((time.perf_counter() - s) * 1e6)
    return statistics.median(times)


# ---------------------------------------------------------------------------
# Benchmark scenarios
# ---------------------------------------------------------------------------

def run_benchmarks(n: int, iters: int, py_iters: int) -> List[Dict[str, Any]]:
    """Run all benchmark scenarios on a dataset of size n."""
    cars = generate_cars(n)

    # Build collection (PRICE uses btree for range queries)
    col = IndexedCollection()
    for attr in [VIN, BRAND, COLOR, YEAR]:
        col.add_index(attr)
    col.add_index(PRICE, index_type="btree")

    t0 = time.perf_counter()
    col.add_many(cars)
    build_s = time.perf_counter() - t0
    build_rate = n / build_s

    results = []

    def add(name, category, pycq_fn, py_fn=None, result_count=None):
        t = bench(pycq_fn, iterations=iters)
        py_us = bench_python(py_fn, iterations=py_iters) if py_fn else None
        speedup = py_us / t["median"] if py_us else None
        entry = {
            "name": name,
            "category": category,
            "n": n,
            "median_us": round(t["median"], 2),
            "p5_us": round(t["p5"], 2),
            "p95_us": round(t["p95"], 2),
        }
        if result_count is not None:
            entry["results"] = result_count
        if py_us is not None:
            entry["python_us"] = round(py_us, 2)
            entry["speedup"] = round(speedup, 1)
        results.append(entry)

    # ---- Build ----
    results.append({
        "name": f"Build {n:,} objects",
        "category": "build",
        "n": n,
        "median_us": round(build_s * 1e6, 0),
        "build_rate": round(build_rate, 0),
    })

    # ---- Point lookup ----
    r = col.retrieve(eq(VIN, n // 2))
    cnt = r.count()
    add("Point lookup (eq VIN)", "point",
        lambda: list(col.retrieve(eq(VIN, n // 2))),
        lambda: [c for c in cars if c.vin == n // 2],
        result_count=cnt)

    # ---- count() no materialization ----
    add("count() eq(BRAND, 'Toyota')", "count",
        lambda: col.retrieve(eq(BRAND, "Toyota")).count(),
        lambda: sum(1 for c in cars if c.brand == "Toyota"),
        result_count=col.retrieve(eq(BRAND, "Toyota")).count())

    # ---- first(10) ----
    add("first(10) eq(BRAND, 'Toyota')", "first",
        lambda: col.retrieve(eq(BRAND, "Toyota")).first(10),
        None,
        result_count=10)

    # ---- 2-way AND (full list) ----
    q2 = and_(eq(BRAND, "Tesla"), eq(COLOR, "Red"))
    cnt2 = col.retrieve(q2).count()
    add("AND 2-way (Brand+Color) list()", "and",
        lambda: list(col.retrieve(q2)),
        lambda: [c for c in cars if c.brand == "Tesla" and c.color == "Red"],
        result_count=cnt2)

    # ---- 3-way AND (full list) ----
    q3 = and_(eq(BRAND, "Toyota"), eq(COLOR, "Blue"), eq(YEAR, 2023))
    cnt3 = col.retrieve(q3).count()
    add("AND 3-way (Brand+Color+Year) list()", "and",
        lambda: list(col.retrieve(q3)),
        lambda: [c for c in cars if c.brand == "Toyota" and c.color == "Blue" and c.year == 2023],
        result_count=cnt3)

    # ---- 4-way AND empty result ----
    q4 = and_(eq(BRAND, "Tesla"), eq(COLOR, "Red"), eq(YEAR, 2023), eq(PRICE, 99999))
    cnt4 = col.retrieve(q4).count()
    add("AND 4-way (empty result)", "and",
        lambda: list(col.retrieve(q4)),
        lambda: [c for c in cars if c.brand == "Tesla" and c.color == "Red" and c.year == 2023 and c.price == 99999],
        result_count=cnt4)

    # ---- OR 2-way ----
    qor2 = or_(eq(BRAND, "Tesla"), eq(BRAND, "Ford"))
    cntor2 = col.retrieve(qor2).count()
    add("OR 2-way (Tesla|Ford) list()", "or",
        lambda: list(col.retrieve(qor2)),
        lambda: [c for c in cars if c.brand in ("Tesla", "Ford")],
        result_count=cntor2)

    # ---- OR 3-way ----
    qor3 = or_(eq(BRAND, "Tesla"), eq(BRAND, "Ford"), eq(BRAND, "BMW"))
    cntor3 = col.retrieve(qor3).count()
    add("OR 3-way (Tesla|Ford|BMW) list()", "or",
        lambda: list(col.retrieve(qor3)),
        lambda: [c for c in cars if c.brand in ("Tesla", "Ford", "BMW")],
        result_count=cntor3)

    # ---- IN query ----
    qin = in_(BRAND, ["Tesla", "Ford", "BMW"])
    cntin = col.retrieve(qin).count()
    add("IN 3-val (Tesla,Ford,BMW) list()", "in",
        lambda: list(col.retrieve(qin)),
        lambda: [c for c in cars if c.brand in ("Tesla", "Ford", "BMW")],
        result_count=cntin)

    # ---- Cache: repeated point lookup ----
    # Do cold miss first, then measure warm hits
    _ = list(col.retrieve(eq(VIN, n // 2)))
    add("Cached point lookup (warm)", "cache",
        lambda: list(col.retrieve(eq(VIN, n // 2))),
        None,
        result_count=1)

    # ---- Range queries (BTree) ----
    # gt: price > 40000 (about 40% of data)
    qgt = gt(PRICE, 40000)
    cntgt = col.retrieve(qgt).count()
    add("gt(PRICE, 40000) list()", "range",
        lambda: list(col.retrieve(qgt)),
        lambda: [c for c in cars if c.price > 40000],
        result_count=cntgt)

    # between: 30000 <= price <= 40000 (about 22% of data)
    qbtw = between(PRICE, 30000, 40000)
    cntbtw = col.retrieve(qbtw).count()
    add("between(PRICE, 30k-40k) list()", "range",
        lambda: list(col.retrieve(qbtw)),
        lambda: [c for c in cars if 30000 <= c.price <= 40000],
        result_count=cntbtw)

    # count() on range (zero-alloc)
    add("count() gt(PRICE, 40000)", "range",
        lambda: col.retrieve(gt(PRICE, 40000)).count(),
        lambda: sum(1 for c in cars if c.price > 40000),
        result_count=cntgt)

    # Narrow range: between 44000-46000 (about 4% of data)
    qnarrow = between(PRICE, 44000, 46000)
    cntnarrow = col.retrieve(qnarrow).count()
    add("between(PRICE, narrow) list()", "range",
        lambda: list(col.retrieve(qnarrow)),
        lambda: [c for c in cars if 44000 <= c.price <= 46000],
        result_count=cntnarrow)

    # AND + range: brand='Tesla' AND price > 35000
    qmix = and_(eq(BRAND, "Tesla"), gt(PRICE, 35000))
    cntmix = col.retrieve(qmix).count()
    add("AND(eq+gt) mixed list()", "range",
        lambda: list(col.retrieve(qmix)),
        lambda: [c for c in cars if c.brand == "Tesla" and c.price > 35000],
        result_count=cntmix)

    return results


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

def print_table(all_results: List[Dict[str, Any]]):
    """Pretty-print results as a table grouped by dataset size."""
    sizes = sorted(set(r["n"] for r in all_results))
    for n in sizes:
        rows = [r for r in all_results if r["n"] == n]
        print(f"\n{'=' * 90}")
        print(f"  Dataset: {n:,} objects")
        print(f"{'=' * 90}")

        # Build row
        build_rows = [r for r in rows if r["category"] == "build"]
        for br in build_rows:
            rate = br.get("build_rate", 0)
            print(f"  Build: {br['median_us']/1e6:.2f}s  ({rate:,.0f} obj/s)")

        print()
        print(f"  {'Scenario':<42} {'Median':>9} {'P5':>9} {'P95':>9} {'Results':>8} {'Python':>9} {'Speedup':>8}")
        print(f"  {'-'*42} {'-'*9} {'-'*9} {'-'*9} {'-'*8} {'-'*9} {'-'*8}")

        for r in rows:
            if r["category"] == "build":
                continue
            med = f"{r['median_us']:.1f}μs"
            p5 = f"{r.get('p5_us', 0):.1f}μs"
            p95 = f"{r.get('p95_us', 0):.1f}μs"
            cnt = str(r.get("results", "")) if "results" in r else ""
            py = f"{r['python_us']:.0f}μs" if "python_us" in r else "-"
            sp = f"{r['speedup']:.1f}x" if "speedup" in r else "-"
            print(f"  {r['name']:<42} {med:>9} {p5:>9} {p95:>9} {cnt:>8} {py:>9} {sp:>8}")

    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="PyCQEngine Unified Benchmark")
    parser.add_argument("--json", action="store_true", help="Output JSON results")
    parser.add_argument("--quick", action="store_true", help="Quick mode (fewer iterations)")
    parser.add_argument("--sizes", type=str, default="100000",
                        help="Comma-separated dataset sizes (default: 100000)")
    args = parser.parse_args()

    sizes = [int(s.strip().replace("_", "")) for s in args.sizes.split(",")]
    iters = 50 if args.quick else 200
    py_iters = 20 if args.quick else 50

    print("=" * 90)
    print("  PyCQEngine Unified Benchmark Suite")
    print(f"  Iterations: {iters} (PyCQEngine), {py_iters} (Python)")
    print(f"  Sizes: {', '.join(f'{s:,}' for s in sizes)}")
    print("=" * 90)

    all_results = []
    for n in sizes:
        all_results.extend(run_benchmarks(n, iters, py_iters))

    print_table(all_results)

    if args.json:
        ts = time.strftime("%Y-%m-%d_%H%M%S")
        fname = f"benchmarks/results_{ts}.json"
        with open(fname, "w") as f:
            json.dump({
                "timestamp": ts,
                "sizes": sizes,
                "iterations": iters,
                "results": all_results,
            }, f, indent=2)
        print(f"  JSON saved to: {fname}")

    return all_results


if __name__ == "__main__":
    main()
