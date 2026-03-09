#!/usr/bin/env python3
"""
PyCQEngine Benchmark Runner
==============================
The challenger: PyCQEngine with hash + BTree indexes.

Usage:
    python benchmarks/competitive/run_pycqengine.py
    python benchmarks/competitive/run_pycqengine.py --quick
    python benchmarks/competitive/run_pycqengine.py --sizes 10000,100000,1000000
"""

import argparse
import sys
import time

sys.path.insert(0, ".")

from benchmarks.competitive.harness import (
    Car, Runner, generate_cars, run_suite, print_multi_size_table,
)
from pycqengine import IndexedCollection, Attribute, eq, and_, or_, in_, gt, between
from typing import Any, List


# Attribute extractors — defined once
_VIN   = Attribute("vin",   lambda c: c.vin)
_BRAND = Attribute("brand", lambda c: c.brand)
_COLOR = Attribute("color", lambda c: c.color)
_PRICE = Attribute("price", lambda c: c.price)
_YEAR  = Attribute("year",  lambda c: c.year)


class PyCQEngineRunner(Runner):
    """PyCQEngine with hash indexes on vin/brand/color/year, BTree on price."""

    @property
    def name(self) -> str:
        return "PyCQEngine"

    def setup(self, cars: List[Car], n: int) -> float:
        self._col = IndexedCollection()
        for attr in [_VIN, _BRAND, _COLOR, _YEAR]:
            self._col.add_index(attr)
        self._col.add_index(_PRICE, index_type="btree")

        self._n = n
        t0 = time.perf_counter()
        self._col.add_many(cars)
        return time.perf_counter() - t0

    def run_scenario(self, scenario_id: str) -> Any:
        col = self._col
        n = self._n

        if scenario_id == "point_lookup":
            return list(col.retrieve(eq(_VIN, n // 2)))

        elif scenario_id == "count_eq":
            return col.retrieve(eq(_BRAND, "Toyota")).count()

        elif scenario_id == "and_2way":
            return list(col.retrieve(and_(eq(_BRAND, "Tesla"), eq(_COLOR, "Red"))))

        elif scenario_id == "and_3way":
            return list(col.retrieve(and_(eq(_BRAND, "Toyota"), eq(_COLOR, "Blue"), eq(_YEAR, 2023))))

        elif scenario_id == "and_4way_empty":
            return list(col.retrieve(and_(eq(_BRAND, "Tesla"), eq(_COLOR, "Red"), eq(_YEAR, 2023), eq(_PRICE, 99999))))

        elif scenario_id == "or_2way":
            return list(col.retrieve(or_(eq(_BRAND, "Tesla"), eq(_BRAND, "Ford"))))

        elif scenario_id == "or_3way":
            return list(col.retrieve(or_(eq(_BRAND, "Tesla"), eq(_BRAND, "Ford"), eq(_BRAND, "BMW"))))

        elif scenario_id == "in_3val":
            return list(col.retrieve(in_(_BRAND, ["Tesla", "Ford", "BMW"])))

        elif scenario_id == "range_gt":
            return list(col.retrieve(gt(_PRICE, 40000)))

        elif scenario_id == "range_between":
            return list(col.retrieve(between(_PRICE, 30000, 40000)))

        elif scenario_id == "range_narrow":
            return list(col.retrieve(between(_PRICE, 44000, 46000)))

        elif scenario_id == "mixed_and":
            return list(col.retrieve(and_(eq(_BRAND, "Tesla"), gt(_PRICE, 35000))))

        else:
            raise ValueError(f"Unknown scenario: {scenario_id}")

    def teardown(self) -> None:
        if hasattr(self, "_col"):
            self._col.clear()
            self._col = None


def main():
    parser = argparse.ArgumentParser(description="PyCQEngine Benchmark Runner")
    parser.add_argument("--quick", action="store_true", help="Fewer iterations")
    parser.add_argument("--sizes", type=str, default="10000,100000,1000000",
                        help="Comma-separated dataset sizes")
    args = parser.parse_args()

    sizes = [int(s.strip().replace("_", "")) for s in args.sizes.split(",")]

    print("=" * 80)
    print("  PyCQEngine Benchmark Runner")
    print(f"  Sizes: {', '.join(f'{s:,}' for s in sizes)}")
    print("=" * 80)

    runner = PyCQEngineRunner()
    suite = run_suite(runner, sizes=sizes, quick=args.quick)
    print_multi_size_table(suite)

    return suite


if __name__ == "__main__":
    main()
