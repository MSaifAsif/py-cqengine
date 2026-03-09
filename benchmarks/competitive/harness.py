"""
Competitive Benchmark Harness
==============================
Shared dataset generation, timing utilities, scenario definitions,
and output formatting used by all competitor runners.

Each competitor implements a Runner subclass with:
  - name: str
  - setup(cars, n) → build the engine/structure
  - run_scenario(scenario_id) → execute one query, return result list/count

Usage from a runner file:
    from benchmarks.competitive.harness import run_suite, Scenario
"""

import abc
import time
import statistics
import sys
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------

class Car:
    """Test object using __slots__ for memory efficiency at scale."""
    __slots__ = ["vin", "brand", "color", "price", "year"]

    def __init__(self, vin: int, brand: str, color: str, price: int, year: int):
        self.vin = vin
        self.brand = brand
        self.color = color
        self.price = price
        self.year = year

    def __repr__(self):
        return f"Car({self.vin})"


BRANDS = ["Tesla", "Ford", "BMW", "Toyota", "Honda", "Mercedes", "Audi", "Nissan"]
COLORS = ["Red", "Blue", "Black", "White", "Silver", "Gray"]
YEARS = [2020, 2021, 2022, 2023, 2024]

# Distribution (deterministic, based on index):
#   brand: 8 values → 12.5% each
#   color: 6 values → 16.7% each
#   year:  5 values → 20% each
#   price: 20000 + (i%100)*500 → 100 distinct values, range [20000, 69500]


def generate_cars(n: int) -> List[Car]:
    """Generate n Car objects with deterministic, uniform distribution."""
    return [
        Car(i, BRANDS[i % 8], COLORS[i % 6], 20000 + (i % 100) * 500, YEARS[i % 5])
        for i in range(n)
    ]


# Expected result counts for validation (per 100K objects, scales linearly):
#   Point lookup (VIN = N//2):          1
#   eq(BRAND, "Toyota"):                N * 1/8 = 12.5%
#   AND(brand=Tesla, color=Red):        N * 1/8 * 1/6 ≈ 2.08%
#   AND(brand=Toyota, color=Blue, yr=2023): N * 1/8 * 1/6 * 1/5 ≈ 0.42%
#   AND(4-way, empty):                  0
#   OR(Tesla | Ford):                   N * 2/8 = 25%
#   OR(Tesla | Ford | BMW):             N * 3/8 = 37.5%
#   IN(Tesla, Ford, BMW):               same as OR 3
#   gt(price, 40000):                   N * 59/100 = 59%
#   between(price, 30000, 40000):       N * 21/100 = 21%
#   between(price, 44000, 46000):       N * 5/100 = 5%  (narrow)
#   AND(brand=Tesla, price > 35000):    N * 1/8 * 69/100 ≈ 8.6%


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------

@dataclass
class Scenario:
    """One benchmark scenario definition."""
    id: str             # Unique key: "point_lookup", "and_2way", etc.
    name: str           # Human-readable: "Point lookup (eq VIN)"
    category: str       # Group: "point", "count", "and", "or", "in", "range"
    selectivity: str    # Readable: "0.001%", "12.5%", etc.


# The canonical scenario list — every runner must implement all of these.
SCENARIOS: List[Scenario] = [
    Scenario("point_lookup",     "Point lookup (eq VIN=N/2)",        "point", "1 result"),
    Scenario("count_eq",         "count() eq(BRAND, Toyota)",        "count", "12.5%"),
    Scenario("and_2way",         "AND(brand=Tesla, color=Red)",      "and",   "~2.1%"),
    Scenario("and_3way",         "AND(brand=Toyota,color=Blue,yr=2023)", "and", "~0.42%"),
    Scenario("and_4way_empty",   "AND(4-way, impossible)",           "and",   "0 results"),
    Scenario("or_2way",          "OR(Tesla | Ford)",                 "or",    "25%"),
    Scenario("or_3way",          "OR(Tesla | Ford | BMW)",           "or",    "37.5%"),
    Scenario("in_3val",          "IN(brand=[Tesla,Ford,BMW])",       "in",    "37.5%"),
    Scenario("range_gt",         "gt(price, 40000)",                 "range", "59%"),
    Scenario("range_between",    "between(price, 30k-40k)",         "range", "21%"),
    Scenario("range_narrow",     "between(price, 44k-46k)",         "range", "5%"),
    Scenario("mixed_and",        "AND(brand=Tesla, price>35000)",   "range", "~8.6%"),
]

SCENARIO_BY_ID = {s.id: s for s in SCENARIOS}


# ---------------------------------------------------------------------------
# Runner interface
# ---------------------------------------------------------------------------

class Runner(abc.ABC):
    """
    Base class for a competitive benchmark runner.

    Subclass and implement:
      - name (property)
      - setup(cars, n) — build the index/structure; return build_time_s
      - run_scenario(scenario_id) — execute the query, return a list (or count for count_eq)
      - teardown() — optional cleanup
    """

    @property
    @abc.abstractmethod
    def name(self) -> str:
        ...

    @abc.abstractmethod
    def setup(self, cars: List[Car], n: int) -> float:
        """Ingest cars into the engine. Return build time in seconds."""
        ...

    @abc.abstractmethod
    def run_scenario(self, scenario_id: str) -> Any:
        """Run the scenario, return the result (list or int for counts)."""
        ...

    def teardown(self) -> None:
        """Optional cleanup after all scenarios."""
        pass


# ---------------------------------------------------------------------------
# Timing
# ---------------------------------------------------------------------------

def _auto_iterations(n: int, quick: bool) -> int:
    """Scale iterations inversely with dataset size to keep total time manageable."""
    if quick:
        base = {10_000: 50, 100_000: 20, 1_000_000: 5, 10_000_000: 3}
    else:
        base = {10_000: 200, 100_000: 100, 1_000_000: 20, 10_000_000: 5}
    # Pick the closest size
    for threshold in sorted(base.keys()):
        if n <= threshold:
            return base[threshold]
    return base[max(base.keys())]


@dataclass
class TimingResult:
    """Timing stats for one scenario + runner combo."""
    scenario_id: str
    runner_name: str
    n: int
    median_us: float
    p5_us: float
    p95_us: float
    result_count: int


def time_scenario(
    runner: Runner,
    scenario_id: str,
    iterations: int,
    warmup: int = 3,
) -> TimingResult:
    """Benchmark a single scenario, return timing + result count."""
    # Warmup
    for _ in range(warmup):
        result = runner.run_scenario(scenario_id)

    # Count results from last warmup
    if isinstance(result, (list, tuple)):
        result_count = len(result)
    elif isinstance(result, int):
        result_count = result
    else:
        result_count = len(list(result))

    # Timed runs
    times: List[float] = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        runner.run_scenario(scenario_id)
        elapsed_us = (time.perf_counter() - t0) * 1e6
        times.append(elapsed_us)

    times.sort()
    return TimingResult(
        scenario_id=scenario_id,
        runner_name=runner.name,
        n=0,  # filled in by caller
        median_us=round(statistics.median(times), 2),
        p5_us=round(times[max(0, len(times) * 5 // 100)], 2),
        p95_us=round(times[min(len(times) - 1, len(times) * 95 // 100)], 2),
        result_count=result_count,
    )


# ---------------------------------------------------------------------------
# Suite runner
# ---------------------------------------------------------------------------

@dataclass
class SuiteResult:
    """Full results for one runner across all sizes."""
    runner_name: str
    # size → scenario_id → TimingResult
    timings: Dict[int, Dict[str, TimingResult]] = field(default_factory=dict)
    # size → build_time_seconds
    build_times: Dict[int, float] = field(default_factory=dict)


def run_suite(
    runner: Runner,
    sizes: List[int] = None,
    quick: bool = False,
    scenarios: List[str] = None,
) -> SuiteResult:
    """
    Run the full benchmark suite for one runner across all sizes.

    Args:
        runner: The Runner instance to benchmark
        sizes: Dataset sizes (default: [10_000, 100_000, 1_000_000])
        quick: If True, use fewer iterations
        scenarios: List of scenario IDs to run (default: all)
    """
    if sizes is None:
        sizes = [10_000, 100_000, 1_000_000]

    scenario_ids = scenarios or [s.id for s in SCENARIOS]
    suite = SuiteResult(runner_name=runner.name)

    for n in sizes:
        print(f"\n  [{runner.name}] Dataset: {n:,} objects")

        # Generate data
        sys.stdout.write(f"    Generating {n:,} cars... ")
        sys.stdout.flush()
        cars = generate_cars(n)
        print("done")

        # Build / ingest
        sys.stdout.write(f"    Building index... ")
        sys.stdout.flush()
        build_time = runner.setup(cars, n)
        suite.build_times[n] = build_time
        rate = n / build_time if build_time > 0 else float("inf")
        print(f"done ({build_time:.3f}s, {rate:,.0f} obj/s)")

        # Run each scenario
        iters = _auto_iterations(n, quick)
        suite.timings[n] = {}

        for sid in scenario_ids:
            scenario = SCENARIO_BY_ID.get(sid)
            if scenario is None:
                continue
            sys.stdout.write(f"    {scenario.name:<45}")
            sys.stdout.flush()

            tr = time_scenario(runner, sid, iterations=iters)
            tr.n = n
            suite.timings[n][sid] = tr

            print(f" {tr.median_us:>10.1f} μs   ({tr.result_count:,} results)")

        runner.teardown()

    return suite


# ---------------------------------------------------------------------------
# Comparison output
# ---------------------------------------------------------------------------

def print_comparison(
    baseline: SuiteResult,
    challenger: SuiteResult,
) -> None:
    """Print a side-by-side comparison table: baseline vs challenger."""
    sizes = sorted(set(baseline.timings.keys()) & set(challenger.timings.keys()))

    print()
    print("=" * 108)
    print(f"  COMPARISON: {baseline.runner_name} vs {challenger.runner_name}")
    print("=" * 108)

    for n in sizes:
        print(f"\n  Dataset: {n:,} objects")

        # Build times
        bt_b = baseline.build_times.get(n, 0)
        bt_c = challenger.build_times.get(n, 0)
        print(f"    Build: {baseline.runner_name}={bt_b:.3f}s  {challenger.runner_name}={bt_c:.3f}s")
        print()

        header = (
            f"  {'Scenario':<40} "
            f"{'Results':>8}  "
            f"{baseline.runner_name:>12}  "
            f"{challenger.runner_name:>12}  "
            f"{'Speedup':>10}"
        )
        print(header)
        print(f"  {'-'*40} {'-'*8}  {'-'*12}  {'-'*12}  {'-'*10}")

        b_timings = baseline.timings.get(n, {})
        c_timings = challenger.timings.get(n, {})

        for scenario in SCENARIOS:
            sid = scenario.id
            if sid not in b_timings or sid not in c_timings:
                continue
            bt = b_timings[sid]
            ct = c_timings[sid]

            speedup = bt.median_us / ct.median_us if ct.median_us > 0 else float("inf")
            if speedup >= 1:
                sp_str = f"{speedup:.1f}x faster"
            else:
                sp_str = f"{1/speedup:.1f}x slower"

            print(
                f"  {scenario.name:<40} "
                f"{bt.result_count:>8,}  "
                f"{bt.median_us:>10.1f}μs  "
                f"{ct.median_us:>10.1f}μs  "
                f"{sp_str:>10}"
            )

    print()
    print("=" * 108)


def print_multi_size_table(suite: SuiteResult) -> None:
    """Print a single runner's results across all sizes in a scaling table."""
    sizes = sorted(suite.timings.keys())
    if not sizes:
        return

    print()
    print("=" * (50 + 22 * len(sizes)))
    print(f"  {suite.runner_name} — Multi-Scale Results")
    print("=" * (50 + 22 * len(sizes)))
    print()

    header = f"  {'Scenario':<45}"
    for n in sizes:
        header += f"  {n:>10,}"
    print(header)
    divider = f"  {'-'*45}"
    for _ in sizes:
        divider += f"  {'-'*10}"
    print(divider)

    for scenario in SCENARIOS:
        sid = scenario.id
        row = f"  {scenario.name:<45}"
        for n in sizes:
            tr = suite.timings.get(n, {}).get(sid)
            if tr is None:
                row += f"  {'—':>10}"
            elif tr.median_us >= 1000:
                row += f"  {tr.median_us/1000:>8.1f}ms"
            else:
                row += f"  {tr.median_us:>8.1f}μs"
        print(row)

    # Build times row
    row = f"  {'Build time':<45}"
    for n in sizes:
        bt = suite.build_times.get(n, 0)
        row += f"  {bt:>8.2f}s "
    print(row)

    print()
