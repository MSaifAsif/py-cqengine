use pyo3::prelude::*;
use std::hash::{Hash, Hasher};

/// Typed value representation for indexing
/// Supports common Python types with efficient comparison and hashing
#[derive(Debug, Clone)]
pub enum TypedValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    None,
}

impl TypedValue {
    /// Convert a Python object to a TypedValue
    pub fn from_py(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        if obj.is_none() {
            Ok(TypedValue::None)
        } else if let Ok(val) = obj.extract::<bool>() {
            Ok(TypedValue::Bool(val))
        } else if let Ok(val) = obj.extract::<i64>() {
            Ok(TypedValue::Int(val))
        } else if let Ok(val) = obj.extract::<f64>() {
            Ok(TypedValue::Float(val))
        } else if let Ok(val) = obj.extract::<String>() {
            Ok(TypedValue::String(val))
        } else {
            // Fallback: convert to string representation
            Ok(TypedValue::String(obj.str()?.to_string()))
        }
    }
}

impl PartialEq for TypedValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (TypedValue::String(a), TypedValue::String(b)) => a == b,
            (TypedValue::Int(a), TypedValue::Int(b)) => a == b,
            (TypedValue::Float(a), TypedValue::Float(b)) => {
                // Handle float comparison with epsilon for near-equality
                (a - b).abs() < f64::EPSILON
            }
            (TypedValue::Bool(a), TypedValue::Bool(b)) => a == b,
            (TypedValue::None, TypedValue::None) => true,
            _ => false,
        }
    }
}

impl Eq for TypedValue {}

impl PartialOrd for TypedValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TypedValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        // Ordering: None < Bool < Int < Float < String
        // Cross-type: compare by discriminant
        let disc = |v: &TypedValue| -> u8 {
            match v {
                TypedValue::None => 0,
                TypedValue::Bool(_) => 1,
                TypedValue::Int(_) => 2,
                TypedValue::Float(_) => 3,
                TypedValue::String(_) => 4,
            }
        };
        let da = disc(self);
        let db = disc(other);
        if da != db {
            return da.cmp(&db);
        }
        match (self, other) {
            (TypedValue::None, TypedValue::None) => Ordering::Equal,
            (TypedValue::Bool(a), TypedValue::Bool(b)) => a.cmp(b),
            (TypedValue::Int(a), TypedValue::Int(b)) => a.cmp(b),
            (TypedValue::Float(a), TypedValue::Float(b)) => {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (TypedValue::String(a), TypedValue::String(b)) => a.cmp(b),
            _ => Ordering::Equal,
        }
    }
}

impl Hash for TypedValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            TypedValue::String(s) => {
                0u8.hash(state);
                s.hash(state);
            }
            TypedValue::Int(i) => {
                1u8.hash(state);
                i.hash(state);
            }
            TypedValue::Float(f) => {
                2u8.hash(state);
                // Hash float as bits to maintain consistency
                f.to_bits().hash(state);
            }
            TypedValue::Bool(b) => {
                3u8.hash(state);
                b.hash(state);
            }
            TypedValue::None => {
                4u8.hash(state);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_typed_value_equality() {
        assert_eq!(TypedValue::Int(42), TypedValue::Int(42));
        assert_eq!(TypedValue::String("test".into()), TypedValue::String("test".into()));
        assert_eq!(TypedValue::Bool(true), TypedValue::Bool(true));
        assert_ne!(TypedValue::Int(42), TypedValue::Int(43));
    }

    #[test]
    fn test_typed_value_hash() {
        use std::collections::hash_map::DefaultHasher;
        
        let mut hasher1 = DefaultHasher::new();
        let mut hasher2 = DefaultHasher::new();
        
        TypedValue::Int(42).hash(&mut hasher1);
        TypedValue::Int(42).hash(&mut hasher2);
        
        assert_eq!(hasher1.finish(), hasher2.finish());
    }
}
