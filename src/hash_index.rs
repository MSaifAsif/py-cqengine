use crate::{Index, TypedValue};
use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::Arc;

/// High-performance concurrent hash index
/// Uses DashMap for lock-free reads and fine-grained write locking
#[derive(Clone)]
pub struct HashIndex {
    // Maps value -> set of object IDs
    index: Arc<DashMap<TypedValue, HashSet<u64>>>,
}

impl HashIndex {
    pub fn new() -> Self {
        Self {
            index: Arc::new(DashMap::new()),
        }
    }
}

impl Default for HashIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl Index for HashIndex {
    fn insert(&self, value: TypedValue, object_id: u64) {
        self.index
            .entry(value)
            .or_insert_with(HashSet::new)
            .insert(object_id);
    }

    fn remove(&self, value: &TypedValue, object_id: u64) {
        if let Some(mut entry) = self.index.get_mut(value) {
            entry.remove(&object_id);
            // Remove empty sets to save memory
            if entry.is_empty() {
                drop(entry);
                self.index.remove(value);
            }
        }
    }

    fn lookup_eq(&self, value: &TypedValue) -> HashSet<u64> {
        self.index
            .get(value)
            .map(|entry| entry.value().clone())
            .unwrap_or_default()
    }

    fn count_eq(&self, value: &TypedValue) -> usize {
        self.index
            .get(value)
            .map(|entry| entry.value().len())
            .unwrap_or(0)
    }

    fn lookup_first(&self, value: &TypedValue, n: usize) -> Vec<u64> {
        self.index
            .get(value)
            .map(|entry| entry.value().iter().take(n).copied().collect())
            .unwrap_or_default()
    }

    fn clear(&self) {
        self.index.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_index_insert_and_lookup() {
        let index = HashIndex::new();
        
        index.insert(TypedValue::String("test".into()), 1);
        index.insert(TypedValue::String("test".into()), 2);
        index.insert(TypedValue::String("other".into()), 3);
        
        let results = index.lookup_eq(&TypedValue::String("test".into()));
        assert_eq!(results.len(), 2);
        assert!(results.contains(&1));
        assert!(results.contains(&2));
        
        let other_results = index.lookup_eq(&TypedValue::String("other".into()));
        assert_eq!(other_results.len(), 1);
        assert!(other_results.contains(&3));
    }

    #[test]
    fn test_hash_index_remove() {
        let index = HashIndex::new();
        
        index.insert(TypedValue::Int(42), 1);
        index.insert(TypedValue::Int(42), 2);
        
        let results = index.lookup_eq(&TypedValue::Int(42));
        assert_eq!(results.len(), 2);
        
        index.remove(&TypedValue::Int(42), 1);
        let results = index.lookup_eq(&TypedValue::Int(42));
        assert_eq!(results.len(), 1);
        assert!(results.contains(&2));
    }

    #[test]
    fn test_hash_index_clear() {
        let index = HashIndex::new();
        
        index.insert(TypedValue::String("test".into()), 1);
        index.insert(TypedValue::Int(42), 2);
        
        index.clear();
        
        assert!(index.lookup_eq(&TypedValue::String("test".into())).is_empty());
        assert!(index.lookup_eq(&TypedValue::Int(42)).is_empty());
    }
}
