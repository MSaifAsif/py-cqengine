use crate::TypedValue;
use std::collections::HashSet;

/// Trait for all index implementations
/// Provides a common interface for different index types (Hash, Range, etc.)
pub trait Index: Send + Sync {
    /// Insert a value mapping to an object ID
    fn insert(&self, value: TypedValue, object_id: u64);
    
    /// Remove a value mapping for an object ID
    fn remove(&self, value: &TypedValue, object_id: u64);
    
    /// Lookup object IDs by exact value match
    fn lookup_eq(&self, value: &TypedValue) -> HashSet<u64>;
    
    /// Count object IDs matching a value WITHOUT cloning the set
    fn count_eq(&self, value: &TypedValue) -> usize;
    
    /// Lookup first N object IDs matching a value (avoids full clone)
    fn lookup_first(&self, value: &TypedValue, n: usize) -> Vec<u64>;
    
    /// Clear all entries from the index
    fn clear(&self);
}
