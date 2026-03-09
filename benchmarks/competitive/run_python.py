#!/usr/bin/env python3
"""
Native Python Benchmark Runner
================================
Baseline competitor: plain list + list comprehension filtering.
No indexes, no libraries — this is what PyCQEngine must beat.

Usage:
    python benchmarks/competitive/run_python.py
    python benchmarks/competitive/run_python.py --quick
    python benchmarks/competitive/run_python.py --sizes 10000,100000,1000000
"""

import argparse
import sys
import time

# Ensure the project root is importable
sys.path.insert(0, ".")

from benchmarks.competitive.harness import (
    Car, Runner, generate_cars, run_suite, print_multi_size_table, SCENARIOS,
)
from typing import Any, List


class NativePythonRunner(Runner):
    """
    Baseline: plain Python list + list comprehensions.
    Build = just store the list reference (no indexing).
    """

    @property
    def name(self) -> str:
        return "Python"

    def setup(self, cars: List[Car], n: int) -> float:
        t0 = time.perf_counter()
        self._cars = cars
        self._n = n
        # Build a dict for point lookup (simulates the best-case Python approach)
        self._by_vin = {c.vin: c for c in cars}
        elapsed = time.perf_counter() - t0
        return elapsed

    def run_scenario(self, scenario_id: str) -> Any:
        cars = self._cars
        n = self._n

        if scenario_id == "point_lookup":
            # Dict lookup — the fastest Python can possibly do
            target = n // 2
            v = self._by_vin.get(target)
            return [v] if v is not None else []

        elif scenario_id == "count_eq":
            return sum(1 for c in cars if c.brand == "Toyota")

        elif scenario_id == "and_2way":
            return [c for c in cars if c.brand == "Tesla" and c.color == "Red"]

        elif scenario_id == "and_3way":
            return [c for c in cars if c.brand == "Toyota" and c.color == "Blue" and c.year == 2023]

        elif scenario_id == "and_4way_empty":
            return [c for c in cars if c.brand == "Tesla" and c.color == "Red" and c.year == 2023 and c.price == 99999]

        elif scenario_id == "or_2way":
            return [c for c in cars if c.brand in ("Tesla", "Ford")]

        elif scenario_id == "or_3way":
            return [c for c in cars if c.brand in ("Tesla", "Ford", "BMW")]

        elif scenario_id == "in_3val":
            brands = {"Tesla", "Ford", "BMW"}
            return [c for c in cars if c.brand in brands]

        elif scenario_id == "range_gt":
            return [c for c in cars if c.price > 40000]

        elif scenario_id == "range_between":
            return [c for c in cars if 30000 <= c.price <= 40000]

        elif scenario_id == "range_narrow":
            return [c for c in cars if 44000 <= c.price <= 46000]

        elif scenario_id == "mixed_and":
            return [c for c in cars if c.brand == "Tesla" and c.price > 35000]

        else:
            raise ValueError(f"Unknown scenario: {scenario_id}")

    def teardown(self) -> None:
        self._cars = None
        self._by_vin = None


def main():
    parser = argparse.ArgumentParser(description="Native Python Benchmark Runner")
    parser.add_argument("--quick", action="store_true", help="Fewer iterations")
    parser.add_argument("--sizes", type=str, default="10000,100000,1000000",
                        help="Comma-separated dataset sizes")
    args = parser.parse_args()

    sizes = [int(s.strip().replace("_", "")) for s in args.sizes.split(",")]

    print("=" * 80)
    print("  Native Python Benchmark Runner")
    print(f"  Sizes: {', '.join(f'{s:,}' for s in sizes)}")
    print("=" * 80)

    runner = NativePythonRunner()
    suite = run_suite(runner, sizes=sizes, quick=args.quick)
    print_multi_size_table(suite)

    return suite


if __name__ == "__main__":
    main()
