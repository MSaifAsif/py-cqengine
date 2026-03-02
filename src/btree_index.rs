use crate::{Index, TypedValue};
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashSet};
use std::ops::Bound;
use std::sync::Arc;

/// Sorted index using BTreeMap for range queries.
///
/// Supports equality lookups (like HashIndex) plus:
///   - gt / gte / lt / lte
///   - between (inclusive/exclusive)
///
/// Trade-off vs HashIndex: O(log n) equality vs O(1), but enables range scans.
#[derive(Clone)]
pub struct BTreeIndex {
    /// Sorted map: value → set of object IDs
    index: Arc<RwLock<BTreeMap<TypedValue, HashSet<u64>>>>,
}

impl BTreeIndex {
    pub fn new() -> Self {
        Self {
            index: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    // ── Range scan helpers ─────────────────────────────────────────────

    /// Return all object IDs where value > threshold
    pub fn lookup_gt(&self, value: &TypedValue) -> HashSet<u64> {
        let guard = self.index.read();
        let mut result = HashSet::new();
        for (_k, ids) in guard.range((Bound::Excluded(value.clone()), Bound::Unbounded)) {
            result.extend(ids);
        }
        result
    }

    /// Return all object IDs where value >= threshold
    pub fn lookup_gte(&self, value: &TypedValue) -> HashSet<u64> {
        let guard = self.index.read();
        let mut result = HashSet::new();
        for (_k, ids) in guard.range((Bound::Included(value.clone()), Bound::Unbounded)) {
            result.extend(ids);
        }
        result
    }

    /// Return all object IDs where value < threshold
    pub fn lookup_lt(&self, value: &TypedValue) -> HashSet<u64> {
        let guard = self.index.read();
        let mut result = HashSet::new();
        for (_k, ids) in guard.range((Bound::Unbounded, Bound::Excluded(value.clone()))) {
            result.extend(ids);
        }
        result
    }

    /// Return all object IDs where value <= threshold
    pub fn lookup_lte(&self, value: &TypedValue) -> HashSet<u64> {
        let guard = self.index.read();
        let mut result = HashSet::new();
        for (_k, ids) in guard.range((Bound::Unbounded, Bound::Included(value.clone()))) {
            result.extend(ids);
        }
        result
    }

    /// Return all object IDs where min <= value <= max
    pub fn lookup_between(&self, min: &TypedValue, max: &TypedValue) -> HashSet<u64> {
        let guard = self.index.read();
        let mut result = HashSet::new();
        for (_k, ids) in guard.range((Bound::Included(min.clone()), Bound::Included(max.clone()))) {
            result.extend(ids);
        }
        result
    }

    /// Count object IDs where value > threshold (no allocation)
    pub fn count_gt(&self, value: &TypedValue) -> usize {
        let guard = self.index.read();
        guard.range((Bound::Excluded(value.clone()), Bound::Unbounded))
            .map(|(_, ids)| ids.len())
            .sum()
    }

    /// Count object IDs where min <= value <= max (no allocation)
    pub fn count_between(&self, min: &TypedValue, max: &TypedValue) -> usize {
        let guard = self.index.read();
        guard.range((Bound::Included(min.clone()), Bound::Included(max.clone())))
            .map(|(_, ids)| ids.len())
            .sum()
    }
}

impl Default for BTreeIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl Index for BTreeIndex {
    fn insert(&self, value: TypedValue, object_id: u64) {
        let mut guard = self.index.write();
        guard.entry(value).or_insert_with(HashSet::new).insert(object_id);
    }

    fn remove(&self, value: &TypedValue, object_id: u64) {
        let mut guard = self.index.write();
        let should_remove = if let Some(ids) = guard.get_mut(value) {
            ids.remove(&object_id);
            ids.is_empty()
        } else {
            false
        };
        if should_remove {
            guard.remove(value);
        }
    }

    fn lookup_eq(&self, value: &TypedValue) -> HashSet<u64> {
        let guard = self.index.read();
        guard.get(value).cloned().unwrap_or_default()
    }

    fn count_eq(&self, value: &TypedValue) -> usize {
        let guard = self.index.read();
        guard.get(value).map(|ids| ids.len()).unwrap_or(0)
    }

    fn lookup_first(&self, value: &TypedValue, n: usize) -> Vec<u64> {
        let guard = self.index.read();
        guard.get(value)
            .map(|ids| ids.iter().take(n).copied().collect())
            .unwrap_or_default()
    }

    fn clear(&self) {
        let mut guard = self.index.write();
        guard.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_equality_lookup() {
        let idx = BTreeIndex::new();
        idx.insert(TypedValue::Int(10), 1);
        idx.insert(TypedValue::Int(20), 2);
        idx.insert(TypedValue::Int(10), 3);

        let r = idx.lookup_eq(&TypedValue::Int(10));
        assert_eq!(r.len(), 2);
        assert!(r.contains(&1) && r.contains(&3));
    }

    #[test]
    fn test_btree_gt() {
        let idx = BTreeIndex::new();
        for i in 0..10 {
            idx.insert(TypedValue::Int(i), i as u64);
        }
        let r = idx.lookup_gt(&TypedValue::Int(7));
        assert_eq!(r, HashSet::from([8, 9]));
    }

    #[test]
    fn test_btree_gte() {
        let idx = BTreeIndex::new();
        for i in 0..10 {
            idx.insert(TypedValue::Int(i), i as u64);
        }
        let r = idx.lookup_gte(&TypedValue::Int(7));
        assert_eq!(r, HashSet::from([7, 8, 9]));
    }

    #[test]
    fn test_btree_lt() {
        let idx = BTreeIndex::new();
        for i in 0..10 {
            idx.insert(TypedValue::Int(i), i as u64);
        }
        let r = idx.lookup_lt(&TypedValue::Int(3));
        assert_eq!(r, HashSet::from([0, 1, 2]));
    }

    #[test]
    fn test_btree_lte() {
        let idx = BTreeIndex::new();
        for i in 0..10 {
            idx.insert(TypedValue::Int(i), i as u64);
        }
        let r = idx.lookup_lte(&TypedValue::Int(3));
        assert_eq!(r, HashSet::from([0, 1, 2, 3]));
    }

    #[test]
    fn test_btree_between() {
        let idx = BTreeIndex::new();
        for i in 0..10 {
            idx.insert(TypedValue::Int(i), i as u64);
        }
        let r = idx.lookup_between(&TypedValue::Int(3), &TypedValue::Int(6));
        assert_eq!(r, HashSet::from([3, 4, 5, 6]));
    }

    #[test]
    fn test_btree_remove() {
        let idx = BTreeIndex::new();
        idx.insert(TypedValue::Int(10), 1);
        idx.insert(TypedValue::Int(10), 2);
        idx.remove(&TypedValue::Int(10), 1);
        let r = idx.lookup_eq(&TypedValue::Int(10));
        assert_eq!(r.len(), 1);
        assert!(r.contains(&2));
    }

    #[test]
    fn test_btree_clear() {
        let idx = BTreeIndex::new();
        idx.insert(TypedValue::Int(1), 1);
        idx.insert(TypedValue::Int(2), 2);
        idx.clear();
        assert!(idx.lookup_eq(&TypedValue::Int(1)).is_empty());
    }

    #[test]
    fn test_btree_count() {
        let idx = BTreeIndex::new();
        for i in 0..100 {
            idx.insert(TypedValue::Int(i % 10), i as u64);
        }
        assert_eq!(idx.count_gt(&TypedValue::Int(8)), 10);       // only value=9
        assert_eq!(idx.count_between(&TypedValue::Int(3), &TypedValue::Int(5)), 30); // 3,4,5 × 10
    }
}
