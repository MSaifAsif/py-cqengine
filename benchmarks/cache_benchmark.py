#!/usr/bin/env python3
"""
Query Result Caching Performance Benchmark
Tests the effectiveness of LRU caching for repeated queries
"""

import time
import statistics
from dataclasses import dataclass
from typing import List
from pycqengine import IndexedCollection, Attribute, eq, and_, or_, in_


@dataclass
class User:
    """User with multiple attributes"""
    id: int
    username: str
    country: str
    status: str
    age_group: str


# Attributes
ID = Attribute("id", lambda u: u.id)
USERNAME = Attribute("username", lambda u: u.username)
COUNTRY = Attribute("country", lambda u: u.country)
STATUS = Attribute("status", lambda u: u.status)
AGE_GROUP = Attribute("age_group", lambda u: u.age_group)


def generate_users(n: int) -> List[User]:
    """Generate test user dataset"""
    countries = ["US", "UK", "DE", "FR", "JP", "CN", "IN", "BR"]
    statuses = ["active", "inactive", "suspended"]
    age_groups = ["18-25", "26-35", "36-45", "46-60", "60+"]
    
    users = []
    for i in range(n):
        users.append(User(
            id=i,
            username=f"user_{i}",
            country=countries[i % len(countries)],
            status=statuses[i % len(statuses)],
            age_group=age_groups[i % len(age_groups)]
        ))
    
    return users


def benchmark_cached_query(collection, query_func, iterations=1000):
    """Benchmark a query with caching (first miss, then hits)"""
    times = []
    
    for i in range(iterations):
        start = time.perf_counter()
        result = query_func()
        end = time.perf_counter()
        times.append((end - start) * 1_000_000)  # microseconds
    
    # First query is cache miss, rest are hits
    first_query_time = times[0]
    cache_hit_times = times[1:]
    
    return {
        'first': first_query_time,
        'median_hit': statistics.median(cache_hit_times),
        'mean_hit': statistics.mean(cache_hit_times),
        'min_hit': min(cache_hit_times),
        'max_hit': max(cache_hit_times),
        'result_count': len(list(result))
    }


def main():
    print("=" * 80)
    print("Query Result Caching Performance Benchmark")
    print("=" * 80)
    print()
    
    # Setup
    n_users = 100_000
    print(f"Dataset: {n_users:,} users")
    print()
    
    print("Generating test data...")
    users = generate_users(n_users)
    print(f"✓ Generated {len(users):,} users")
    print()
    
    # Build indexed collection
    print("Building indexed collection...")
    collection = IndexedCollection()
    collection.add_index(ID)
    collection.add_index(USERNAME)
    collection.add_index(COUNTRY)
    collection.add_index(STATUS)
    collection.add_index(AGE_GROUP)
    
    start = time.time()
    collection.add_many(users)
    elapsed = time.time() - start
    print(f"✓ Indexed {len(collection):,} users in {elapsed:.2f}s")
    print()
    
    print("=" * 80)
    print("TEST 1: Simple Equality Query (Repeated 1,000x)")
    print("=" * 80)
    print()
    print("Query: country='US' (repeated 1,000 times)")
    print()
    
    stats = benchmark_cached_query(
        collection,
        lambda: list(collection.retrieve(eq(COUNTRY, "US"))),
        iterations=1000
    )
    
    speedup = stats['first'] / stats['median_hit']
    
    print(f"Results: {stats['result_count']:,} users")
    print()
    print(f"First query (cache miss):  {stats['first']:>10.2f} μs")
    print(f"Median (cache hit):         {stats['median_hit']:>10.2f} μs")
    print(f"Mean (cache hit):           {stats['mean_hit']:>10.2f} μs")
    print(f"Min (cache hit):            {stats['min_hit']:>10.2f} μs")
    print(f"Max (cache hit):            {stats['max_hit']:>10.2f} μs")
    print()
    print(f"Speedup (hit vs miss):      {speedup:>10.1f}x {'⚡' * min(5, int(speedup)//100)}")
    print()
    
    print("=" * 80)
    print("TEST 2: AND Query (Repeated 1,000x)")
    print("=" * 80)
    print()
    print("Query: country='US' AND status='active' (repeated 1,000 times)")
    print()
    
    stats = benchmark_cached_query(
        collection,
        lambda: list(collection.retrieve(and_(
            eq(COUNTRY, "US"),
            eq(STATUS, "active")
        ))),
        iterations=1000
    )
    
    speedup = stats['first'] / stats['median_hit']
    
    print(f"Results: {stats['result_count']:,} users")
    print()
    print(f"First query (cache miss):  {stats['first']:>10.2f} μs")
    print(f"Median (cache hit):         {stats['median_hit']:>10.2f} μs")
    print(f"Speedup (hit vs miss):      {speedup:>10.1f}x {'⚡' * min(5, int(speedup)//100)}")
    print()
    
    print("=" * 80)
    print("TEST 3: OR Query (Repeated 1,000x)")
    print("=" * 80)
    print()
    print("Query: country='US' OR country='UK' (repeated 1,000 times)")
    print()
    
    stats = benchmark_cached_query(
        collection,
        lambda: list(collection.retrieve(or_(
            eq(COUNTRY, "US"),
            eq(COUNTRY, "UK")
        ))),
        iterations=1000
    )
    
    speedup = stats['first'] / stats['median_hit']
    
    print(f"Results: {stats['result_count']:,} users")
    print()
    print(f"First query (cache miss):  {stats['first']:>10.2f} μs")
    print(f"Median (cache hit):         {stats['median_hit']:>10.2f} μs")
    print(f"Speedup (hit vs miss):      {speedup:>10.1f}x {'⚡' * min(5, int(speedup)//100)}")
    print()
    
    print("=" * 80)
    print("TEST 4: IN Query (Repeated 1,000x)")
    print("=" * 80)
    print()
    print("Query: country IN ['US', 'UK', 'DE'] (repeated 1,000 times)")
    print()
    
    stats = benchmark_cached_query(
        collection,
        lambda: list(collection.retrieve(in_(COUNTRY, ["US", "UK", "DE"]))),
        iterations=1000
    )
    
    speedup = stats['first'] / stats['median_hit']
    
    print(f"Results: {stats['result_count']:,} users")
    print()
    print(f"First query (cache miss):  {stats['first']:>10.2f} μs")
    print(f"Median (cache hit):         {stats['median_hit']:>10.2f} μs")
    print(f"Speedup (hit vs miss):      {speedup:>10.1f}x {'⚡' * min(5, int(speedup)//100)}")
    print()
    
    print("=" * 80)
    print("TEST 5: Multiple Different Queries (Cache Thrashing)")
    print("=" * 80)
    print()
    print("Cycling through 10 different queries, 100 iterations each")
    print()
    
    # Define 10 different queries
    queries = [
        lambda: list(collection.retrieve(eq(COUNTRY, "US"))),
        lambda: list(collection.retrieve(eq(COUNTRY, "UK"))),
        lambda: list(collection.retrieve(eq(COUNTRY, "DE"))),
        lambda: list(collection.retrieve(eq(STATUS, "active"))),
        lambda: list(collection.retrieve(eq(STATUS, "inactive"))),
        lambda: list(collection.retrieve(and_(eq(COUNTRY, "US"), eq(STATUS, "active")))),
        lambda: list(collection.retrieve(and_(eq(COUNTRY, "UK"), eq(STATUS, "active")))),
        lambda: list(collection.retrieve(or_(eq(COUNTRY, "US"), eq(COUNTRY, "UK")))),
        lambda: list(collection.retrieve(in_(COUNTRY, ["US", "UK", "DE"]))),
        lambda: list(collection.retrieve(eq(AGE_GROUP, "26-35"))),
    ]
    
    times = []
    for i in range(1000):
        query_func = queries[i % len(queries)]
        start = time.perf_counter()
        result = query_func()
        end = time.perf_counter()
        times.append((end - start) * 1_000_000)
    
    # First 10 are cache misses, rest are hits (cycling)
    first_round = times[:10]
    subsequent_rounds = times[10:]
    
    print(f"First round (10 cache misses): {statistics.mean(first_round):>10.2f} μs (avg)")
    print(f"Subsequent hits (990 queries):  {statistics.mean(subsequent_rounds):>10.2f} μs (avg)")
    print(f"Speedup:                        {statistics.mean(first_round) / statistics.mean(subsequent_rounds):>10.1f}x")
    print()
    
    print("=" * 80)
    print("TEST 6: Very Selective Query (Point Lookup)")
    print("=" * 80)
    print()
    print("Query: id=50000 (repeated 1,000 times)")
    print()
    
    stats = benchmark_cached_query(
        collection,
        lambda: list(collection.retrieve(eq(ID, 50000))),
        iterations=1000
    )
    
    speedup = stats['first'] / stats['median_hit']
    
    print(f"Results: {stats['result_count']:,} user(s)")
    print()
    print(f"First query (cache miss):  {stats['first']:>10.2f} μs")
    print(f"Median (cache hit):         {stats['median_hit']:>10.2f} μs")
    print(f"Speedup (hit vs miss):      {speedup:>10.1f}x {'⚡' * min(5, int(speedup)//100)}")
    print()


if __name__ == "__main__":
    main()
