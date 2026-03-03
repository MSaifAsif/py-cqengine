use crate::{BTreeIndex, HashIndex, Index, TypedValue};
use dashmap::DashMap;
use lru::LruCache;
use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::PyWeakrefReference;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::num::NonZeroUsize;
use std::sync::Arc;

/// Specification for a single sub-query inside a general AND/OR.
/// Used to pass mixed query types (eq + range) in one FFI call.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum QuerySpec {
    Eq(String, TypedValue),
    Gt(String, TypedValue),
    Gte(String, TypedValue),
    Lt(String, TypedValue),
    Lte(String, TypedValue),
    Between(String, TypedValue, TypedValue),
}

/// Cache key for query results
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum CacheKey {
    Eq(String, TypedValue),
    And(Vec<(String, TypedValue)>),
    AndGeneral(Vec<QuerySpec>),
    Or(Vec<(String, TypedValue)>),
    In(String, Vec<TypedValue>),
    Gt(String, TypedValue),
    Gte(String, TypedValue),
    Lt(String, TypedValue),
    Lte(String, TypedValue),
    Between(String, TypedValue, TypedValue),
}

/// Wrapper enum so we can store Hash and BTree indexes in the same map
#[derive(Clone)]
enum IndexKind {
    Hash(Arc<HashIndex>),
    BTree(Arc<BTreeIndex>),
}

/// Index reference needed by a QuerySpec during execution.
/// For eq we can use any IndexKind; for range ops we need BTreeIndex.
enum IndexRef {
    Any(IndexKind),
    BTree(Arc<BTreeIndex>),
}

impl IndexKind {
    fn as_index(&self) -> &dyn Index {
        match self {
            IndexKind::Hash(h) => h.as_ref(),
            IndexKind::BTree(b) => b.as_ref(),
        }
    }

    /// Static-dispatch lookup_eq — avoids vtable indirection
    #[inline]
    fn lookup_eq(&self, value: &TypedValue) -> HashSet<u64> {
        match self {
            IndexKind::Hash(h) => Index::lookup_eq(h.as_ref(), value),
            IndexKind::BTree(b) => Index::lookup_eq(b.as_ref(), value),
        }
    }

    /// Static-dispatch count_eq — avoids vtable indirection
    #[inline]
    fn count_eq(&self, value: &TypedValue) -> usize {
        match self {
            IndexKind::Hash(h) => Index::count_eq(h.as_ref(), value),
            IndexKind::BTree(b) => Index::count_eq(b.as_ref(), value),
        }
    }

    fn as_btree(&self) -> Option<&Arc<BTreeIndex>> {
        match self {
            IndexKind::BTree(b) => Some(b),
            _ => None,
        }
    }
}

/// Central collection manager for Python objects
/// Maintains object registry and manages indexes
#[pyclass]
pub struct CollectionManager {
    // Object ID -> Python object reference (or weakref in weak mode)
    objects: Arc<DashMap<u64, PyObject>>,
    
    // Attribute name -> Index implementation (Hash or BTree)
    indexes: Arc<DashMap<String, IndexKind>>,
    
    // Query result cache (LRU with 1000 entries)
    query_cache: Arc<Mutex<LruCache<CacheKey, Vec<u64>>>>,

    // Whether to store weak references instead of strong ones
    use_weakrefs: bool,

    // Reverse index: object_id -> [(attr_name, typed_value)] for gc() cleanup
    // Only populated when use_weakrefs=true
    reverse_index: Arc<DashMap<u64, Vec<(String, TypedValue)>>>,
}

#[pymethods]
impl CollectionManager {
    #[new]
    #[pyo3(signature = (use_weakrefs=false))]
    pub fn new(use_weakrefs: bool) -> Self {
        Self {
            objects: Arc::new(DashMap::new()),
            indexes: Arc::new(DashMap::new()),
            query_cache: Arc::new(Mutex::new(
                LruCache::new(NonZeroUsize::new(1000).unwrap())
            )),
            use_weakrefs,
            reverse_index: Arc::new(DashMap::new()),
        }
    }

    /// Add an index for a specific attribute.
    /// index_type: "hash" (default, O(1) eq) or "btree" (O(log n) eq + range)
    #[pyo3(signature = (attribute_name, index_type="hash"))]
    pub fn add_index(&self, attribute_name: String, index_type: &str) {
        let kind = match index_type {
            "btree" => IndexKind::BTree(Arc::new(BTreeIndex::new())),
            _ => IndexKind::Hash(Arc::new(HashIndex::new())),
        };
        self.indexes.insert(attribute_name, kind);
    }

    /// Add an object to the collection with extracted attribute values
    /// 
    /// Args:
    ///     obj: Python object to store
    ///     attributes: Dict mapping attribute names to extracted values
    pub fn add_object(
        &self,
        py: Python<'_>,
        obj: PyObject,
        attributes: HashMap<String, Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let object_id = obj.as_ptr() as u64;
        
        // Convert attribute values to typed
        let mut typed_attrs: Vec<(String, TypedValue)> = Vec::new();
        for (attr_name, value) in &attributes {
            if self.indexes.contains_key(attr_name) {
                let tv = TypedValue::from_py(value)?;
                typed_attrs.push((attr_name.clone(), tv));
            }
        }

        // Store the object reference (strong or weak)
        if self.use_weakrefs {
            // Check for dead ref at same address (address reuse cleanup)
            self.maybe_cleanup_dead_ref(py, object_id);
            // Try to create weakref; fall back to strong ref
            let stored = match PyWeakrefReference::new(obj.bind(py)) {
                Ok(weakref) => weakref.unbind().into(),
                Err(_) => obj,
            };
            self.objects.insert(object_id, stored);
            self.reverse_index.insert(object_id, typed_attrs.clone());
        } else {
            self.objects.insert(object_id, obj);
        }
        
        // Update all indexes
        for (attr_name, typed_value) in &typed_attrs {
            if let Some(index) = self.indexes.get(attr_name) {
                let tv = typed_value.clone();
                py.allow_threads(|| {
                    index.as_index().insert(tv, object_id);
                });
            }
        }
        
        // Invalidate cache on data modification
        self.query_cache.lock().clear();
        
        Ok(())
    }

    /// Add multiple objects in batch with parallel index insertion
    pub fn add_objects_batch(
        &self,
        py: Python<'_>,
        objects: Vec<(PyObject, HashMap<String, Bound<'_, PyAny>>)>,
    ) -> PyResult<()> {
        if objects.is_empty() {
            return Ok(());
        }

        // Phase 1: Extract typed values and store objects (sequential, needs GIL)
        let mut index_entries: Vec<(u64, Vec<(String, TypedValue)>)> = Vec::with_capacity(objects.len());

        for (obj, attributes) in objects {
            let object_id = obj.as_ptr() as u64;
            let mut typed_attrs = Vec::new();
            for (attr_name, value) in attributes {
                if self.indexes.contains_key(&attr_name) {
                    let typed_value = TypedValue::from_py(&value)?;
                    typed_attrs.push((attr_name, typed_value));
                }
            }

            if self.use_weakrefs {
                self.maybe_cleanup_dead_ref(py, object_id);
                let stored = match PyWeakrefReference::new(obj.bind(py)) {
                    Ok(weakref) => weakref.unbind().into(),
                    Err(_) => obj,
                };
                self.objects.insert(object_id, stored);
                self.reverse_index.insert(object_id, typed_attrs.clone());
            } else {
                self.objects.insert(object_id, obj);
            }

            index_entries.push((object_id, typed_attrs));
        }

        // Phase 2: Parallel index insertion (GIL released)
        let indexes = &self.indexes;
        py.allow_threads(|| {
            index_entries.par_iter().for_each(|(object_id, typed_attrs)| {
                for (attr_name, typed_value) in typed_attrs {
                    if let Some(index) = indexes.get(attr_name) {
                        index.as_index().insert(typed_value.clone(), *object_id);
                    }
                }
            });
        });

        // Invalidate cache once (not per object)
        self.query_cache.lock().clear();

        Ok(())
    }

    /// Count matching objects WITHOUT cloning the ID set (zero-allocation)
    pub fn query_eq_count(&self, py: Python<'_>, attribute: String, value: Bound<'_, PyAny>) -> PyResult<usize> {
        let typed_value = TypedValue::from_py(&value)?;
        
        let index = self.indexes
            .get(&attribute)
            .ok_or_else(|| {
                pyo3::exceptions::PyKeyError::new_err(format!("No index for attribute: {}", attribute))
            })?;
        
        let count = py.allow_threads(|| {
            index.as_index().count_eq(&typed_value)
        });
        
        Ok(count)
    }

    /// Get first N matching objects in a single Rust call (no full ID clone)
    pub fn query_eq_first_objects(
        &self,
        py: Python<'_>,
        attribute: String,
        value: Bound<'_, PyAny>,
        limit: usize,
    ) -> PyResult<Vec<PyObject>> {
        let typed_value = TypedValue::from_py(&value)?;
        
        let index = self.indexes
            .get(&attribute)
            .ok_or_else(|| {
                pyo3::exceptions::PyKeyError::new_err(format!("No index for attribute: {}", attribute))
            })?;
        
        // Only get first N IDs — no full HashSet clone!
        let ids = py.allow_threads(|| {
            index.as_index().lookup_first(&typed_value, limit)
        });
        
        self.ids_to_objects(py, &ids)
    }

    /// Query for object IDs matching a specific attribute value (with caching)
    pub fn query_eq(&self, py: Python<'_>, attribute: String, value: Bound<'_, PyAny>) -> PyResult<Vec<u64>> {
        let typed_value = TypedValue::from_py(&value)?;
        let cache_key = CacheKey::Eq(attribute.clone(), typed_value.clone());
        
        // Check cache first
        {
            let mut cache = self.query_cache.lock();
            if let Some(cached_result) = cache.get(&cache_key) {
                return Ok(cached_result.clone());
            }
        }
        
        // Cache miss - execute query
        let index = self.indexes
            .get(&attribute)
            .ok_or_else(|| {
                pyo3::exceptions::PyKeyError::new_err(format!("No index for attribute: {}", attribute))
            })?;
        
        // Release GIL during query execution
        let result_set = py.allow_threads(|| {
            index.as_index().lookup_eq(&typed_value)
        });
        
        let result: Vec<u64> = result_set.into_iter().collect();
        
        // Store in cache
        {
            let mut cache = self.query_cache.lock();
            cache.put(cache_key, result.clone());
        }
        
        Ok(result)
    }

    /// Fused eq query + object materialization in a single FFI call.
    pub fn query_eq_objects(&self, py: Python<'_>, attribute: String, value: Bound<'_, PyAny>) -> PyResult<Vec<PyObject>> {
        let ids = self.query_eq(py, attribute, value)?;
        self.ids_to_objects(py, &ids)
    }

    /// Query with AND logic (intersection) - performs set operations in Rust (with caching)
    /// 
    /// Args:
    ///     queries: Vec of (attribute_name, value) tuples
    /// 
    /// Returns:
    ///     Vec of object IDs matching ALL queries
    pub fn query_and(
        &self,
        py: Python<'_>,
        queries: Vec<(String, Bound<'_, PyAny>)>,
    ) -> PyResult<Vec<u64>> {
        if queries.is_empty() {
            return Ok(Vec::new());
        }

        // Build cache key
        let typed_queries: Vec<(String, TypedValue)> = queries
            .iter()
            .map(|(attr, val)| Ok((attr.clone(), TypedValue::from_py(val)?)))
            .collect::<PyResult<_>>()?;
        
        let cache_key = CacheKey::And(typed_queries.clone());
        
        // Check cache
        {
            let mut cache = self.query_cache.lock();
            if let Some(cached_result) = cache.get(&cache_key) {
                return Ok(cached_result.clone());
            }
        }

        // Collect IndexKind Arc clones upfront (validates existence, cheap Arc clone)
        let index_clones: Vec<IndexKind> = typed_queries.iter()
            .map(|(attr, _)| {
                self.indexes.get(attr)
                    .map(|r| r.value().clone())
                    .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(
                        format!("No index for attribute: {}", attr)
                    ))
            })
            .collect::<PyResult<_>>()?;

        // Single GIL release for the entire operation (lookup + intersect)
        let result_vec: Vec<u64> = py.allow_threads(|| {
            if typed_queries.len() >= 4 {
                // Parallel: fetch all results at once, then intersect
                let mut all_results: Vec<HashSet<u64>> = typed_queries.par_iter()
                    .zip(index_clones.par_iter())
                    .map(|((_, typed_value), idx)| idx.lookup_eq(typed_value))
                    .collect();
                all_results.sort_by_key(|s| s.len());
                let mut result = all_results.remove(0);
                for other in &all_results {
                    result.retain(|id| other.contains(id));
                    if result.is_empty() { break; }
                }
                result.into_iter().collect()
            } else {
                // Sequential streaming intersection (2-3 conditions)
                let mut result = index_clones[0].lookup_eq(&typed_queries[0].1);
                for i in 1..typed_queries.len() {
                    if result.is_empty() { break; }
                    let other = index_clones[i].lookup_eq(&typed_queries[i].1);
                    result.retain(|id| other.contains(id));
                }
                result.into_iter().collect()
            }
        });
        
        // Store in cache
        {
            let mut cache = self.query_cache.lock();
            cache.put(cache_key, result_vec.clone());
        }
        
        Ok(result_vec)
    }

    /// Fused AND query + object materialization in a single FFI call.
    /// Avoids the intermediate Vec<u64> → Python list → Vec<u64> roundtrip.
    pub fn query_and_objects(
        &self,
        py: Python<'_>,
        queries: Vec<(String, Bound<'_, PyAny>)>,
    ) -> PyResult<Vec<PyObject>> {
        let ids = self.query_and(py, queries)?;
        self.ids_to_objects(py, &ids)
    }

    /// General AND query supporting mixed query types (eq, gt, gte, lt, lte, between).
    ///
    /// Each query_spec is a Python tuple: ("op", "attr", value [, max_value])
    /// where op is one of: "eq", "gt", "gte", "lt", "lte", "between".
    /// "between" requires a 4th element (max_value).
    ///
    /// Returns Vec of object IDs matching ALL sub-queries.
    pub fn query_and_general(
        &self,
        py: Python<'_>,
        query_specs: Vec<Bound<'_, PyAny>>,
    ) -> PyResult<Vec<u64>> {
        if query_specs.is_empty() {
            return Ok(Vec::new());
        }

        // Parse specs from Python tuples
        let specs = self.parse_query_specs(&query_specs)?;

        // Check cache
        let cache_key = CacheKey::AndGeneral(specs.clone());
        {
            let mut cache = self.query_cache.lock();
            if let Some(cached) = cache.get(&cache_key) {
                return Ok(cached.clone());
            }
        }

        // Validate & collect index references up front
        let index_refs = self.collect_index_refs_for_specs(&specs)?;

        // Single GIL release: execute all sub-queries + intersect
        let result_vec: Vec<u64> = py.allow_threads(|| {
            let mut sets: Vec<HashSet<u64>> = specs.iter()
                .zip(index_refs.iter())
                .map(|(spec, iref)| Self::execute_spec(spec, iref))
                .collect();

            // Sort ascending by size → intersect starting from smallest
            sets.sort_by_key(|s| s.len());

            if sets.is_empty() {
                return Vec::new();
            }
            let mut result = sets.remove(0);
            for other in &sets {
                result.retain(|id| other.contains(id));
                if result.is_empty() { break; }
            }
            result.into_iter().collect()
        });

        {
            let mut cache = self.query_cache.lock();
            cache.put(cache_key, result_vec.clone());
        }

        Ok(result_vec)
    }

    /// Fused general AND query + object materialization.
    pub fn query_and_general_objects(
        &self,
        py: Python<'_>,
        query_specs: Vec<Bound<'_, PyAny>>,
    ) -> PyResult<Vec<PyObject>> {
        let ids = self.query_and_general(py, query_specs)?;
        self.ids_to_objects(py, &ids)
    }

    /// Query with OR logic (union) - performs set operations in Rust (with caching)
    /// 
    /// Args:
    ///     queries: Vec of (attribute_name, value) tuples
    /// 
    /// Returns:
    ///     Vec of object IDs matching ANY query
    pub fn query_or(
        &self,
        py: Python<'_>,
        queries: Vec<(String, Bound<'_, PyAny>)>,
    ) -> PyResult<Vec<u64>> {
        // Build cache key
        let typed_queries: Vec<(String, TypedValue)> = queries
            .iter()
            .map(|(attr, val)| Ok((attr.clone(), TypedValue::from_py(val)?)))
            .collect::<PyResult<_>>()?;
        
        let cache_key = CacheKey::Or(typed_queries.clone());
        
        // Check cache
        {
            let mut cache = self.query_cache.lock();
            if let Some(cached_result) = cache.get(&cache_key) {
                return Ok(cached_result.clone());
            }
        }

        // Collect IndexKind Arc clones upfront (validates existence)
        let index_clones: Vec<IndexKind> = typed_queries.iter()
            .map(|(attr, _)| {
                self.indexes.get(attr)
                    .map(|r| r.value().clone())
                    .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(
                        format!("No index for attribute: {}", attr)
                    ))
            })
            .collect::<PyResult<_>>()?;

        let all_same_attr = typed_queries.iter().all(|(attr, _)| attr == &typed_queries[0].0);
        let use_parallel = typed_queries.len() >= 6;

        // Single GIL release for entire operation
        let result_vec: Vec<u64> = py.allow_threads(|| {
            if all_same_attr {
                if use_parallel {
                    let chunks: Vec<Vec<u64>> = typed_queries.par_iter()
                        .zip(index_clones.par_iter())
                        .map(|((_, tv), idx)| idx.lookup_eq(tv).into_iter().collect::<Vec<_>>())
                        .collect();
                    chunks.into_iter().flatten().collect()
                } else {
                    let mut result = Vec::new();
                    for (i, (_, tv)) in typed_queries.iter().enumerate() {
                        result.extend(index_clones[i].lookup_eq(tv));
                    }
                    result
                }
            } else {
                if use_parallel {
                    let chunks: Vec<HashSet<u64>> = typed_queries.par_iter()
                        .zip(index_clones.par_iter())
                        .map(|((_, tv), idx)| idx.lookup_eq(tv))
                        .collect();
                    let mut result = HashSet::new();
                    for chunk in chunks {
                        result.extend(chunk);
                    }
                    result.into_iter().collect()
                } else {
                    let mut result = HashSet::new();
                    for (i, (_, tv)) in typed_queries.iter().enumerate() {
                        result.extend(index_clones[i].lookup_eq(tv));
                    }
                    result.into_iter().collect()
                }
            }
        });
        
        // Store in cache
        {
            let mut cache = self.query_cache.lock();
            cache.put(cache_key, result_vec.clone());
        }
        
        Ok(result_vec)
    }

    /// Fused OR query + object materialization in a single FFI call.
    pub fn query_or_objects(
        &self,
        py: Python<'_>,
        queries: Vec<(String, Bound<'_, PyAny>)>,
    ) -> PyResult<Vec<PyObject>> {
        let ids = self.query_or(py, queries)?;
        self.ids_to_objects(py, &ids)
    }

    /// Query with IN logic (optimized union for single attribute) (with caching)
    /// 
    /// Args:
    ///     attribute: Attribute name
    ///     values: Vec of values to match
    /// 
    /// Returns:
    ///     Vec of object IDs matching any value
    pub fn query_in(
        &self,
        py: Python<'_>,
        attribute: String,
        values: Vec<Bound<'_, PyAny>>,
    ) -> PyResult<Vec<u64>> {
        // Build cache key
        let typed_values: Vec<TypedValue> = values
            .iter()
            .map(|val| TypedValue::from_py(val))
            .collect::<PyResult<_>>()?;
        
        let cache_key = CacheKey::In(attribute.clone(), typed_values.clone());
        
        // Check cache
        {
            let mut cache = self.query_cache.lock();
            if let Some(cached_result) = cache.get(&cache_key) {
                return Ok(cached_result.clone());
            }
        }

        // Cache miss - execute query
        let index_ref = self.indexes
            .get(&attribute)
            .ok_or_else(|| {
                pyo3::exceptions::PyKeyError::new_err(format!("No index for attribute: {}", attribute))
            })?;
        let index = index_ref.value().clone(); // IndexKind clone (cheap Arc inside)
        drop(index_ref); // Release DashMap lock

        // IN on single attribute = disjoint results; single GIL release
        let result_vec: Vec<u64> = py.allow_threads(|| {
            if typed_values.len() >= 6 {
                let chunks: Vec<Vec<u64>> = typed_values.par_iter()
                    .map(|tv| index.lookup_eq(tv).into_iter().collect::<Vec<_>>())
                    .collect();
                chunks.into_iter().flatten().collect()
            } else {
                let mut result = Vec::new();
                for tv in &typed_values {
                    result.extend(index.lookup_eq(tv));
                }
                result
            }
        });
        
        // Store in cache
        {
            let mut cache = self.query_cache.lock();
            cache.put(cache_key, result_vec.clone());
        }
        
        Ok(result_vec)
    }

    /// Fused IN query + object materialization in a single FFI call.
    pub fn query_in_objects(
        &self,
        py: Python<'_>,
        attribute: String,
        values: Vec<Bound<'_, PyAny>>,
    ) -> PyResult<Vec<PyObject>> {
        let ids = self.query_in(py, attribute, values)?;
        self.ids_to_objects(py, &ids)
    }

    // ── Range query methods ───────────────────────────────────────────

    /// Query for objects where attribute > value (requires btree index)
    pub fn query_gt(&self, py: Python<'_>, attribute: String, value: Bound<'_, PyAny>) -> PyResult<Vec<u64>> {
        let tv = TypedValue::from_py(&value)?;
        let ck = CacheKey::Gt(attribute.clone(), tv.clone());
        self.range_query_cached(py, ck, &attribute, |idx| idx.lookup_gt(&tv))
    }

    /// Query for objects where attribute >= value (requires btree index)
    pub fn query_gte(&self, py: Python<'_>, attribute: String, value: Bound<'_, PyAny>) -> PyResult<Vec<u64>> {
        let tv = TypedValue::from_py(&value)?;
        let ck = CacheKey::Gte(attribute.clone(), tv.clone());
        self.range_query_cached(py, ck, &attribute, |idx| idx.lookup_gte(&tv))
    }

    /// Query for objects where attribute < value (requires btree index)
    pub fn query_lt(&self, py: Python<'_>, attribute: String, value: Bound<'_, PyAny>) -> PyResult<Vec<u64>> {
        let tv = TypedValue::from_py(&value)?;
        let ck = CacheKey::Lt(attribute.clone(), tv.clone());
        self.range_query_cached(py, ck, &attribute, |idx| idx.lookup_lt(&tv))
    }

    /// Query for objects where attribute <= value (requires btree index)
    pub fn query_lte(&self, py: Python<'_>, attribute: String, value: Bound<'_, PyAny>) -> PyResult<Vec<u64>> {
        let tv = TypedValue::from_py(&value)?;
        let ck = CacheKey::Lte(attribute.clone(), tv.clone());
        self.range_query_cached(py, ck, &attribute, |idx| idx.lookup_lte(&tv))
    }

    /// Query for objects where min <= attribute <= max (requires btree index)
    pub fn query_between(
        &self,
        py: Python<'_>,
        attribute: String,
        min_val: Bound<'_, PyAny>,
        max_val: Bound<'_, PyAny>,
    ) -> PyResult<Vec<u64>> {
        let tv_min = TypedValue::from_py(&min_val)?;
        let tv_max = TypedValue::from_py(&max_val)?;
        let ck = CacheKey::Between(attribute.clone(), tv_min.clone(), tv_max.clone());
        self.range_query_cached(py, ck, &attribute, |idx| idx.lookup_between(&tv_min, &tv_max))
    }

    /// Count objects where attribute > value (zero-allocation)
    pub fn query_gt_count(&self, py: Python<'_>, attribute: String, value: Bound<'_, PyAny>) -> PyResult<usize> {
        let tv = TypedValue::from_py(&value)?;
        let btree = self.require_btree(&attribute)?;
        Ok(py.allow_threads(|| btree.count_gt(&tv)))
    }

    /// Count objects where min <= attribute <= max (zero-allocation)
    pub fn query_between_count(
        &self,
        py: Python<'_>,
        attribute: String,
        min_val: Bound<'_, PyAny>,
        max_val: Bound<'_, PyAny>,
    ) -> PyResult<usize> {
        let tv_min = TypedValue::from_py(&min_val)?;
        let tv_max = TypedValue::from_py(&max_val)?;
        let btree = self.require_btree(&attribute)?;
        Ok(py.allow_threads(|| btree.count_between(&tv_min, &tv_max)))
    }

    /// Fused range query + object materialization helpers
    pub fn query_gt_objects(&self, py: Python<'_>, attribute: String, value: Bound<'_, PyAny>) -> PyResult<Vec<PyObject>> {
        let ids = self.query_gt(py, attribute, value)?;
        self.ids_to_objects(py, &ids)
    }

    pub fn query_gte_objects(&self, py: Python<'_>, attribute: String, value: Bound<'_, PyAny>) -> PyResult<Vec<PyObject>> {
        let ids = self.query_gte(py, attribute, value)?;
        self.ids_to_objects(py, &ids)
    }

    pub fn query_lt_objects(&self, py: Python<'_>, attribute: String, value: Bound<'_, PyAny>) -> PyResult<Vec<PyObject>> {
        let ids = self.query_lt(py, attribute, value)?;
        self.ids_to_objects(py, &ids)
    }

    pub fn query_lte_objects(&self, py: Python<'_>, attribute: String, value: Bound<'_, PyAny>) -> PyResult<Vec<PyObject>> {
        let ids = self.query_lte(py, attribute, value)?;
        self.ids_to_objects(py, &ids)
    }

    pub fn query_between_objects(
        &self,
        py: Python<'_>,
        attribute: String,
        min_val: Bound<'_, PyAny>,
        max_val: Bound<'_, PyAny>,
    ) -> PyResult<Vec<PyObject>> {
        let ids = self.query_between(py, attribute, min_val, max_val)?;
        self.ids_to_objects(py, &ids)
    }

    // ── Object retrieval ────────────────────────────────────────────

    /// Retrieve Python objects by their IDs (optimized: uses resolve_object for weakref support)
    pub fn get_objects(&self, py: Python<'_>, object_ids: Vec<u64>) -> PyResult<Vec<PyObject>> {
        self.ids_to_objects(py, &object_ids)
    }

    /// Retrieve a slice of Python objects by their IDs (for pagination)
    /// Only materializes objects in the [start, end) range
    pub fn get_objects_slice(
        &self,
        py: Python<'_>,
        object_ids: Vec<u64>,
        start: usize,
        end: usize,
    ) -> PyResult<Vec<PyObject>> {
        let end = end.min(object_ids.len());
        if start >= end {
            return Ok(Vec::new());
        }
        self.ids_to_objects(py, &object_ids[start..end])
    }

    /// Get the total number of objects in the collection
    pub fn size(&self) -> usize {
        self.objects.len()
    }

    /// Clear all objects and indexes (also clears query cache)
    pub fn clear(&self) {
        self.objects.clear();
        self.reverse_index.clear();
        for index in self.indexes.iter() {
            index.value().as_index().clear();
        }
        self.query_cache.lock().clear();
    }
    
    /// Clear only the query cache (useful after data modifications)
    pub fn clear_cache(&self) {
        self.query_cache.lock().clear();
    }
    
    /// Get cache statistics for monitoring
    pub fn cache_stats(&self) -> (usize, usize) {
        let cache = self.query_cache.lock();
        (cache.len(), cache.cap().get())
    }

    /// Whether this collection uses weak references
    #[getter]
    pub fn use_weakrefs(&self) -> bool {
        self.use_weakrefs
    }

    /// Count alive objects (checks weakrefs in weak_refs mode)
    pub fn alive_count(&self, py: Python<'_>) -> usize {
        if !self.use_weakrefs {
            return self.objects.len();
        }
        self.objects.iter()
            .filter(|entry| {
                let bound = entry.value().bind(py);
                match bound.downcast::<PyWeakrefReference>() {
                    Ok(weakref) => weakref.upgrade().is_some(),
                    Err(_) => true, // strong ref fallback
                }
            })
            .count()
    }

    /// Garbage-collect dead weak references and clean their index entries.
    /// Returns the number of dead references cleaned.
    /// No-op in strong-ref mode.
    pub fn gc(&self, py: Python<'_>) -> usize {
        if !self.use_weakrefs {
            return 0;
        }

        let mut dead_ids: Vec<u64> = Vec::new();
        for entry in self.objects.iter() {
            let id = *entry.key();
            let bound = entry.value().bind(py);
            if let Ok(weakref) = bound.downcast::<PyWeakrefReference>() {
                if weakref.upgrade().is_none() {
                    dead_ids.push(id);
                }
            }
        }

        let count = dead_ids.len();
        for id in &dead_ids {
            self.cleanup_single_dead_ref(*id);
        }
        if count > 0 {
            self.query_cache.lock().clear();
        }
        count
    }

    /// Remove a single object from the collection and all indexes
    pub fn remove_object(
        &self,
        py: Python<'_>,
        obj: PyObject,
        attributes: HashMap<String, Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let object_id = obj.as_ptr() as u64;

        // Convert to typed values while GIL is held
        let typed_attrs: Vec<(String, TypedValue)> = attributes.into_iter()
            .filter_map(|(name, val)| {
                TypedValue::from_py(&val).ok().map(|tv| (name, tv))
            })
            .collect();

        // Remove from indexes (GIL released)
        let indexes = &self.indexes;
        py.allow_threads(|| {
            for (attr_name, typed_value) in &typed_attrs {
                if let Some(index) = indexes.get(attr_name) {
                    index.as_index().remove(typed_value, object_id);
                }
            }
        });

        // Remove from object store (GIL held for safe PyObject drop)
        let removed = self.objects.remove(&object_id).is_some();

        // Remove from reverse index
        if self.use_weakrefs {
            self.reverse_index.remove(&object_id);
        }

        // Invalidate cache
        self.query_cache.lock().clear();

        Ok(removed)
    }

    /// Remove multiple objects in batch with parallel index cleanup
    pub fn remove_objects_batch(
        &self,
        py: Python<'_>,
        objects: Vec<(PyObject, HashMap<String, Bound<'_, PyAny>>)>,
    ) -> PyResult<usize> {
        if objects.is_empty() {
            return Ok(0);
        }

        // Phase 1: Convert to typed values (sequential, needs GIL for Bound)
        let mut typed_batch: Vec<(u64, Vec<(String, TypedValue)>)> = Vec::with_capacity(objects.len());
        for (obj, attributes) in objects {
            let object_id = obj.as_ptr() as u64;
            let typed_attrs: Vec<(String, TypedValue)> = attributes.into_iter()
                .filter_map(|(name, val)| {
                    TypedValue::from_py(&val).ok().map(|tv| (name, tv))
                })
                .collect();
            typed_batch.push((object_id, typed_attrs));
        }

        // Phase 2: Parallel index removal (GIL released)
        let indexes = &self.indexes;
        py.allow_threads(|| {
            typed_batch.par_iter().for_each(|(object_id, typed_attrs)| {
                for (attr_name, typed_value) in typed_attrs {
                    if let Some(index) = indexes.get(attr_name) {
                        index.as_index().remove(typed_value, *object_id);
                    }
                }
            });
        });

        // Phase 3: Remove from object store (sequential, GIL held)
        let mut count = 0;
        for (object_id, _) in &typed_batch {
            if self.objects.remove(object_id).is_some() {
                count += 1;
            }
            if self.use_weakrefs {
                self.reverse_index.remove(object_id);
            }
        }

        // Invalidate cache
        self.query_cache.lock().clear();

        Ok(count)
    }
}

/// Internal (non-PyO3) helpers — can use `impl Trait` params
impl CollectionManager {
    /// Get a BTreeIndex for the attribute, or return a descriptive error
    fn require_btree(&self, attribute: &str) -> PyResult<Arc<BTreeIndex>> {
        let index_kind = self.indexes
            .get(attribute)
            .ok_or_else(|| {
                pyo3::exceptions::PyKeyError::new_err(format!("No index for attribute: {}", attribute))
            })?;
        index_kind.as_btree().cloned().ok_or_else(|| {
            pyo3::exceptions::PyTypeError::new_err(
                format!("Attribute '{}' uses a Hash index; range queries require a BTree index", attribute)
            )
        })
    }

    /// Generic cached range query: check cache → run closure → store result
    fn range_query_cached(
        &self,
        py: Python<'_>,
        cache_key: CacheKey,
        attribute: &str,
        f: impl FnOnce(&BTreeIndex) -> HashSet<u64> + Send,
    ) -> PyResult<Vec<u64>> {
        {
            let mut cache = self.query_cache.lock();
            if let Some(cached) = cache.get(&cache_key) {
                return Ok(cached.clone());
            }
        }

        let btree = self.require_btree(attribute)?;
        let result_set = py.allow_threads(|| f(&btree));
        let result: Vec<u64> = result_set.into_iter().collect();

        {
            let mut cache = self.query_cache.lock();
            cache.put(cache_key, result.clone());
        }
        Ok(result)
    }

    /// Shared helper: convert IDs to Python objects (with weakref support + pre-allocation)
    #[inline]
    fn ids_to_objects(&self, py: Python<'_>, ids: &[u64]) -> PyResult<Vec<PyObject>> {
        let mut result = Vec::with_capacity(ids.len());

        if self.use_weakrefs {
            let mut dead_ids: Vec<u64> = Vec::new();
            for &id in ids {
                match self.resolve_object(py, id) {
                    Some(obj) => result.push(obj),
                    None => dead_ids.push(id),
                }
            }
            // Lazy cleanup: remove dead weakrefs from objects + indexes
            if !dead_ids.is_empty() {
                for &id in &dead_ids {
                    self.cleanup_single_dead_ref(id);
                }
                self.query_cache.lock().clear();
            }
        } else {
            for &id in ids {
                if let Some(entry) = self.objects.get(&id) {
                    result.push(entry.value().clone_ref(py));
                }
            }
        }

        Ok(result)
    }

    /// Resolve a single object ID → PyObject, handling both strong refs and weakrefs.
    /// Returns None if the weakref is dead (object has been GC'd).
    #[inline]
    fn resolve_object(&self, py: Python<'_>, id: u64) -> Option<PyObject> {
        let entry = self.objects.get(&id)?;
        let stored = entry.value();

        if self.use_weakrefs {
            let bound = stored.bind(py);
            if let Ok(weakref) = bound.downcast::<PyWeakrefReference>() {
                return weakref.upgrade().map(|obj| obj.unbind());
            }
        }
        // Strong ref path (default, or fallback for objects that don't support weakrefs)
        Some(stored.clone_ref(py))
    }

    /// Remove a dead ref from the object store + all its index entries using the reverse index.
    fn cleanup_single_dead_ref(&self, id: u64) {
        self.objects.remove(&id);
        if let Some((_, attrs)) = self.reverse_index.remove(&id) {
            for (attr_name, typed_value) in &attrs {
                if let Some(index) = self.indexes.get(attr_name) {
                    index.as_index().remove(typed_value, id);
                }
            }
        }
    }

    /// Check if there's a dead weakref at the given ID and clean it up.
    /// Used during add_object to handle address reuse.
    fn maybe_cleanup_dead_ref(&self, py: Python<'_>, id: u64) {
        if let Some(entry) = self.objects.get(&id) {
            let bound = entry.value().bind(py);
            if let Ok(weakref) = bound.downcast::<PyWeakrefReference>() {
                if weakref.upgrade().is_none() {
                    drop(entry);
                    self.cleanup_single_dead_ref(id);
                }
            }
        }
    }

    /// Parse a list of Python query spec tuples into QuerySpec enums.
    /// Each spec is ("op", "attr", value) or ("between", "attr", min, max).
    fn parse_query_specs(&self, specs: &[Bound<'_, PyAny>]) -> PyResult<Vec<QuerySpec>> {
        specs.iter().map(|spec| {
            let tuple = spec.downcast::<pyo3::types::PyTuple>().map_err(|_| {
                pyo3::exceptions::PyTypeError::new_err("query_spec must be a tuple")
            })?;
            let len = tuple.len();
            if len < 3 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "query_spec tuple must have at least 3 elements (op, attr, value)"
                ));
            }
            let op: String = tuple.get_item(0)?.extract()?;
            let attr: String = tuple.get_item(1)?.extract()?;
            let val = TypedValue::from_py(&tuple.get_item(2)?)?;

            match op.as_str() {
                "eq" => Ok(QuerySpec::Eq(attr, val)),
                "gt" => Ok(QuerySpec::Gt(attr, val)),
                "gte" => Ok(QuerySpec::Gte(attr, val)),
                "lt" => Ok(QuerySpec::Lt(attr, val)),
                "lte" => Ok(QuerySpec::Lte(attr, val)),
                "between" => {
                    if len < 4 {
                        return Err(pyo3::exceptions::PyValueError::new_err(
                            "between spec requires 4 elements: ('between', attr, min, max)"
                        ));
                    }
                    let max_val = TypedValue::from_py(&tuple.get_item(3)?)?;
                    Ok(QuerySpec::Between(attr, val, max_val))
                }
                _ => Err(pyo3::exceptions::PyValueError::new_err(
                    format!("Unknown query op: '{}'. Expected eq/gt/gte/lt/lte/between", op)
                )),
            }
        }).collect()
    }

    /// Validate indexes exist & are the right type for each spec.
    fn collect_index_refs_for_specs(&self, specs: &[QuerySpec]) -> PyResult<Vec<IndexRef>> {
        specs.iter().map(|spec| {
            let attr = match spec {
                QuerySpec::Eq(a, _) => a,
                QuerySpec::Gt(a, _) | QuerySpec::Gte(a, _)
                | QuerySpec::Lt(a, _) | QuerySpec::Lte(a, _) => a,
                QuerySpec::Between(a, _, _) => a,
            };
            match spec {
                QuerySpec::Eq(_, _) => {
                    let idx = self.indexes.get(attr)
                        .map(|r| r.value().clone())
                        .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(
                            format!("No index for attribute: {}", attr)
                        ))?;
                    Ok(IndexRef::Any(idx))
                }
                _ => {
                    // Range query — requires BTree
                    let btree = self.require_btree(attr)?;
                    Ok(IndexRef::BTree(btree))
                }
            }
        }).collect()
    }

    /// Execute a single QuerySpec against its pre-validated index reference.
    #[inline]
    fn execute_spec(spec: &QuerySpec, iref: &IndexRef) -> HashSet<u64> {
        match (spec, iref) {
            (QuerySpec::Eq(_, v), IndexRef::Any(idx)) => idx.lookup_eq(v),
            (QuerySpec::Gt(_, v), IndexRef::BTree(bt)) => bt.lookup_gt(v),
            (QuerySpec::Gte(_, v), IndexRef::BTree(bt)) => bt.lookup_gte(v),
            (QuerySpec::Lt(_, v), IndexRef::BTree(bt)) => bt.lookup_lt(v),
            (QuerySpec::Lte(_, v), IndexRef::BTree(bt)) => bt.lookup_lte(v),
            (QuerySpec::Between(_, min, max), IndexRef::BTree(bt)) => bt.lookup_between(min, max),
            // Shouldn't happen if collect_index_refs_for_specs is correct
            _ => HashSet::new(),
        }
    }
}

impl Default for CollectionManager {
    fn default() -> Self {
        Self::new(false)
    }
}
