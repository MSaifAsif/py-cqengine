#!/usr/bin/env python3
"""
Competitive Benchmark: PyCQEngine vs Native Python
=====================================================
Runs both runners on the same scenarios and dataset sizes,
then prints a side-by-side comparison table.

Usage:
    python benchmarks/competitive/compare.py
    python benchmarks/competitive/compare.py --quick
    python benchmarks/competitive/compare.py --sizes 10000,100000,1000000
    python benchmarks/competitive/compare.py --sizes 10000,100000,1000000,10000000
"""

import argparse
import sys

sys.path.insert(0, ".")

from benchmarks.competitive.harness import (
    run_suite, print_comparison, print_multi_size_table, SCENARIOS,
)
from benchmarks.competitive.run_python import NativePythonRunner
from benchmarks.competitive.run_pycqengine import PyCQEngineRunner


def main():
    parser = argparse.ArgumentParser(
        description="Competitive Benchmark: PyCQEngine vs Native Python"
    )
    parser.add_argument("--quick", action="store_true", help="Fewer iterations per scenario")
    parser.add_argument("--sizes", type=str, default="10000,100000,1000000",
                        help="Comma-separated dataset sizes (default: 10K,100K,1M)")
    args = parser.parse_args()

    sizes = [int(s.strip().replace("_", "")) for s in args.sizes.split(",")]

    print("=" * 108)
    print("  PyCQEngine Competitive Benchmark Suite")
    print(f"  Competitors: PyCQEngine vs Native Python")
    print(f"  Sizes: {', '.join(f'{s:,}' for s in sizes)}")
    print("=" * 108)

    # --- Run PyCQEngine ---
    print("\n" + "=" * 80)
    print("  Phase 1: PyCQEngine")
    print("=" * 80)
    pycq_runner = PyCQEngineRunner()
    pycq_suite = run_suite(pycq_runner, sizes=sizes, quick=args.quick)

    # --- Run Native Python ---
    print("\n" + "=" * 80)
    print("  Phase 2: Native Python")
    print("=" * 80)
    py_runner = NativePythonRunner()
    py_suite = run_suite(py_runner, sizes=sizes, quick=args.quick)

    # --- Individual multi-scale tables ---
    print_multi_size_table(pycq_suite)
    print_multi_size_table(py_suite)

    # --- Head-to-head comparison ---
    print_comparison(py_suite, pycq_suite)


if __name__ == "__main__":
    main()
