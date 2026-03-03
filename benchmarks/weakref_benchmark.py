"""Benchmark: weakref mode overhead vs strong mode."""

import time
import statistics
from pycqengine import IndexedCollection, Attribute, eq, and_, or_, in_


class Car:
    def __init__(self, vin, brand, color, year, price):
        self.vin = vin
        self.brand = brand
        self.color = color
        self.year = year
        self.price = price

VIN   = Attribute("vin",   lambda c: c.vin)
BRAND = Attribute("brand", lambda c: c.brand)
COLOR = Attribute("color", lambda c: c.color)
YEAR  = Attribute("year",  lambda c: c.year)
PRICE = Attribute("price", lambda c: c.price)

BRANDS = ["Tesla", "Ford", "BMW", "Toyota", "Honda", "Audi", "Mercedes", "Volvo"]
COLORS = ["Red", "Blue", "Black", "White", "Silver", "Green"]

N = 100_000
ITERS = 200


def build(use_weakrefs):
    col = IndexedCollection(use_weakrefs=use_weakrefs)
    for attr in [VIN, BRAND, COLOR, YEAR, PRICE]:
        col.add_index(attr)
    cars = [
        Car(i, BRANDS[i % len(BRANDS)], COLORS[i % len(COLORS)],
            2015 + (i % 10), 20000 + (i % 50) * 1000)
        for i in range(N)
    ]
    col.add_many(cars)
    return col, cars


def bench(label, fn, iters=ITERS):
    # warmup
    for _ in range(5):
        fn()
    times = []
    for _ in range(iters):
        t0 = time.perf_counter_ns()
        fn()
        times.append(time.perf_counter_ns() - t0)
    med = statistics.median(times) / 1000
    return med


def run():
    print(f"{'Scenario':<45} {'Strong (μs)':>12} {'Weak (μs)':>12} {'Overhead':>10}")
    print("-" * 85)

    col_s, cars_s = build(use_weakrefs=False)
    col_w, cars_w = build(use_weakrefs=True)

    scenarios = [
        ("Point lookup eq(VIN)",
         lambda: list(col_s.retrieve(eq(VIN, 42))),
         lambda: list(col_w.retrieve(eq(VIN, 42)))),
        ("AND 2-way list()",
         lambda: list(col_s.retrieve(and_(eq(BRAND, "Toyota"), eq(COLOR, "Red")))),
         lambda: list(col_w.retrieve(and_(eq(BRAND, "Toyota"), eq(COLOR, "Red"))))),
        ("OR 2-way list()",
         lambda: list(col_s.retrieve(or_(eq(BRAND, "Tesla"), eq(BRAND, "Ford")))),
         lambda: list(col_w.retrieve(or_(eq(BRAND, "Tesla"), eq(BRAND, "Ford"))))),
        ("IN 3-val list()",
         lambda: list(col_s.retrieve(in_(BRAND, ["Tesla", "Ford", "BMW"]))),
         lambda: list(col_w.retrieve(in_(BRAND, ["Tesla", "Ford", "BMW"])))),
        ("count() eq(BRAND)",
         lambda: col_s.retrieve(eq(BRAND, "Toyota")).count(),
         lambda: col_w.retrieve(eq(BRAND, "Toyota")).count()),
        ("alive_count",
         lambda: col_s.alive_count,
         lambda: col_w.alive_count),
        ("gc()",
         lambda: col_s.gc(),
         lambda: col_w.gc()),
    ]

    for label, fn_s, fn_w in scenarios:
        med_s = bench(label, fn_s)
        med_w = bench(label, fn_w)
        overhead = f"{(med_w / med_s - 1) * 100:+.1f}%"
        print(f"  {label:<43} {med_s:>10.1f}   {med_w:>10.1f}   {overhead:>10}")

    # Build time
    for mode, weakrefs in [("Strong", False), ("Weak", True)]:
        times = []
        for _ in range(5):
            col = IndexedCollection(use_weakrefs=weakrefs)
            for attr in [VIN, BRAND, COLOR, YEAR, PRICE]:
                col.add_index(attr)
            cars = [
                Car(i, BRANDS[i % len(BRANDS)], COLORS[i % len(COLORS)],
                    2015 + (i % 10), 20000 + (i % 50) * 1000)
                for i in range(N)
            ]
            t0 = time.perf_counter()
            col.add_many(cars)
            times.append(time.perf_counter() - t0)
        med = statistics.median(times)
        print(f"  Build {N:,} ({mode}): {med:.3f}s  ({N/med:,.0f} obj/s)")


if __name__ == "__main__":
    run()
