# PyCQEngine

High-performance in-memory NoSQL indexing engine for Python object collections, powered by Rust.

> The project is in development phase and is provided as-is for now.

**Performance:** Sub-microsecond point lookups. 100x+ faster than list comprehensions for selective queries on 1,000,000+ objects.

## Features

- **🚀 Blazing Fast**: Rust-backed hash & BTree indexing with sub-1μs point lookups
- **🔒 Thread-Safe**: Lock-free concurrent indexing using DashMap + parking_lot
- **💡 Simple API**: Intuitive query DSL — `eq`, `and_`, `or_`, `in_`, `gt`, `lt`, `between`
- **⚡ Fused Materialization**: Query + object retrieval in a single Rust→Python call
- **🌲 Range Queries**: BTree indexes for `gt` / `gte` / `lt` / `lte` / `between`
- **🔄 Parallel Execution**: Rayon-powered parallel index operations with GIL release
- **📦 Batch Ingestion**: `add_many()` for efficient bulk loading (~330K obj/s)
- **🗑️ Memory Lifecycle**: `remove()`, `remove_many()`, `clear()`, `__del__` support
- **🎯 Zero-Cost Counting**: `count()` and `first(n)` without materializing objects
- **💾 LRU Query Cache**: Automatic caching of repeated queries (1,000 entries)
- **🔗 Weak References**: Opt-in `use_weakrefs=True` mode — objects auto-cleaned when Python GC'd

## Architecture

```
┌─────────────────────────────────────────┐
│         Python Application              │
│  (User Code + Query DSL)                │
└──────────────┬──────────────────────────┘
               │ PyO3 FFI Boundary
┌──────────────▼──────────────────────────┐
│         Rust Core Engine                │
│  • CollectionManager (Object Registry)  │
│  • HashIndex  (DashMap — O(1) eq)       │
│  • BTreeIndex (BTreeMap — range scans)  │
│  • Fused query_*_objects() methods      │
│  • Rayon parallel intersection/union    │
│  • LRU query cache (parking_lot Mutex)  │
│  • GIL Release (True parallelism)       │
└─────────────────────────────────────────┘
```

**Key Design Principles:**
1. **Attribute Extraction**: Lambda extractors run once during `add()`, bypassing Python's `tp_getattro` overhead during queries
2. **Fused Materialization**: Queries execute + materialize objects in a single FFI call, eliminating the IDs→Python→Rust roundtrip
3. **GIL Release**: Index operations release the GIL for true multi-core parallelism
4. **Static Dispatch**: `IndexKind` enum avoids vtable overhead for hot-path lookups

## Installation

### Prerequisites

- Python 3.11+
- Rust 1.70+ (install via [rustup](https://rustup.rs/))

### From Source

```bash
# Clone the repository
git clone https://github.com/yourusername/py-cqengine.git
cd py-cqengine

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install maturin
pip install maturin

# Build and install
maturin develop --release
```

## Quick Start

```python
from pycqengine import IndexedCollection, Attribute, eq, and_, gt, between

class Car:
    def __init__(self, vin, brand, price):
        self.vin = vin
        self.brand = brand
        self.price = price

# Step 1: Define Attributes (lambda extractors)
VIN = Attribute("vin", lambda c: c.vin)
BRAND = Attribute("brand", lambda c: c.brand)
PRICE = Attribute("price", lambda c: c.price)

# Step 2: Setup Collection
cars = IndexedCollection()
cars.add_index(VIN)                          # Hash index (default)
cars.add_index(BRAND)                        # Hash index
cars.add_index(PRICE, index_type="btree")    # BTree index for range queries

# Step 3: Load Data (use add_many for batch efficiency)
cars.add_many([
    Car(1, "Tesla", 50000),
    Car(2, "Ford", 30000),
    Car(3, "Tesla", 60000),
    Car(4, "BMW", 45000),
])

# Step 4: Query
results = cars.retrieve(eq(BRAND, "Tesla"))
for car in results:
    print(f"VIN: {car.vin}, Brand: {car.brand}, Price: ${car.price}")

# Count without materializing objects
count = cars.retrieve(eq(BRAND, "Tesla")).count()  # ~0.9μs

# First N results
top3 = cars.retrieve(eq(BRAND, "Tesla")).first(3)  # ~1.2μs
```

## Query DSL

### Equality Query

```python
from pycqengine import eq

# Find all Teslas
results = cars.retrieve(eq(BRAND, "Tesla"))
```

### AND Query (Intersection)

```python
from pycqengine import and_, eq, gt

# Find Teslas priced above $55,000
results = cars.retrieve(and_(
    eq(BRAND, "Tesla"),
    gt(PRICE, 55000)
))
```

### OR Query (Union)

```python
from pycqengine import or_, eq

# Find Tesla or Ford vehicles
results = cars.retrieve(or_(
    eq(BRAND, "Tesla"),
    eq(BRAND, "Ford")
))
```

### IN Query (Membership)

```python
from pycqengine import in_

# Find vehicles from specific brands
results = cars.retrieve(in_(BRAND, ["Tesla", "Ford", "BMW"]))
```

### Range Queries (requires BTree index)

```python
from pycqengine import gt, gte, lt, lte, between

# Price > 40,000
results = cars.retrieve(gt(PRICE, 40000))

# Price >= 30,000
results = cars.retrieve(gte(PRICE, 30000))

# Price < 50,000
results = cars.retrieve(lt(PRICE, 50000))

# 30,000 <= Price <= 50,000 (inclusive)
results = cars.retrieve(between(PRICE, 30000, 50000))
```

### Memory Management

```python
# Remove a single object
cars.remove(car_obj)

# Remove multiple objects
cars.remove_many([car1, car2, car3])

# Clear entire collection
cars.clear()
```

### Weak References

By default, `IndexedCollection` holds **strong references** to objects, keeping them alive as long as the collection exists. Enable weak reference mode to let Python's GC reclaim objects when no other references exist:

```python
# Opt-in weak reference mode
cars = IndexedCollection(use_weakrefs=True)
cars.add_index(BRAND)
cars.add_index(PRICE, index_type="btree")

car = Car(1, "Tesla", 50000)
cars.add(car)

# Object is retrievable while reference exists
assert list(cars.retrieve(eq(BRAND, "Tesla"))) == [car]

# Drop the reference — Python GC can reclaim it
del car

# Explicit garbage collection
cleaned = cars.gc()       # Returns number of dead refs cleaned
print(cars.alive_count)   # Number of still-alive objects

# Dead refs are also cleaned lazily during queries
results = list(cars.retrieve(eq(BRAND, "Tesla")))  # Returns [] — dead ref auto-cleaned
```

**Notes:**
- Objects that don't support weakrefs (tuples, ints, etc.) automatically fall back to strong refs
- Query performance has **zero overhead** in weakref mode
- Build throughput is ~13% slower (weakref creation + reverse index population)
- `gc()` and `alive_count` scan all objects — suitable for periodic maintenance, not hot loops

## Performance

Benchmarked on macOS ARM64 (Apple Silicon), Python 3.14, Rust 1.93.

### 100K Objects

| Scenario | Median | Results | vs Python |
|----------|--------|---------|-----------|
| Point lookup (eq VIN) | **0.8 μs** | 1 | **3,290x** |
| count() eq(BRAND) | **0.9 μs** | 12,500 | **2,377x** |
| first(10) eq(BRAND) | **1.2 μs** | 10 | — |
| AND 2-way list() | **94 μs** | 4,167 | **22x** |
| AND 3-way list() | **19 μs** | 833 | **110x** |
| AND 4-way (empty result) | **2.4 μs** | 0 | **923x** |
| OR 2-way list() | **535 μs** | 25,000 | **6.0x** |
| IN 3-val list() | **773 μs** | 37,500 | **4.6x** |
| gt(PRICE, 40000) list() | **1,173 μs** | 59,000 | **1.7x** |
| between(30k-40k) list() | **425 μs** | 21,000 | **7.2x** |
| count() gt(PRICE) | **0.6 μs** | 59,000 | **4,087x** |
| between(narrow) list() | **102 μs** | 5,000 | **26.7x** |
| AND(eq+gt) mixed list() | **173 μs** | 8,500 | **12.3x** |
| Build time | **0.30s** | — | 334K obj/s |

### Scaling to 1M Objects

| Scenario | 100K | 500K | 1M |
|----------|------|------|----|
| Point lookup | 0.8μs (3,290x) | 0.8μs (16,824x) | 0.8μs (**33,654x**) |
| count() eq | 0.9μs (2,377x) | 1.0μs (11,441x) | 0.9μs (**23,313x**) |
| AND 3-way | 19μs (110x) | 97μs (114x) | 215μs (**104x**) |
| AND 4-way empty | 2.4μs (923x) | 2.3μs (4,794x) | 2.3μs (**9,663x**) |
| count() gt | 0.6μs (4,087x) | 0.6μs (22,686x) | 0.6μs (**43,840x**) |
| between(narrow) | 102μs (26.7x) | 537μs (26.6x) | 1,527μs (**18.9x**) |
| Build throughput | 334K obj/s | 337K obj/s | 331K obj/s |

> Point lookups, counts, and empty-result queries are **O(1)** — speedup scales linearly with collection size.
> Selective queries (AND, narrow range) remain 10–100x+ faster at all scales.

## Development

### Project Structure

```
py-cqengine/
├── src/                    # Rust source code
│   ├── lib.rs             # PyO3 module initialization
│   ├── types.rs           # TypedValue enum (str/int/float/bool)
│   ├── collection.rs      # CollectionManager + query methods
│   ├── index.rs           # Index trait (lookup, insert, remove)
│   ├── hash_index.rs      # DashMap-based O(1) equality index
│   └── btree_index.rs     # BTreeMap-based range index
├── python/pycqengine/     # Python package
│   ├── __init__.py        # Public API exports
│   ├── core.py            # IndexedCollection + ResultSet
│   ├── attribute.py       # Attribute extractor
│   └── query.py           # Query DSL (eq, and_, or_, in_, gt, between...)
├── tests/                 # Python tests (119 tests)
├── benchmarks/            # Performance benchmarks
├── Cargo.toml             # Rust dependencies
└── pyproject.toml         # Python package config
```

### Build Commands

```bash
# Development build (with debug symbols)
maturin develop

# Release build (optimized)
maturin develop --release

# Run Python tests
python -m pytest tests/ -v

# Run benchmarks
python benchmarks/run_all.py                           # Standard (100K)
python benchmarks/run_all.py --sizes 100000,500000     # Multi-scale
python benchmarks/run_all.py --quick                   # Fast iteration
python benchmarks/run_all.py --json                    # Save JSON for diffing
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - see LICENSE file for details.
