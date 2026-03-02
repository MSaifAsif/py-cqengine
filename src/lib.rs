use pyo3::prelude::*;

mod types;
mod collection;
mod index;
mod hash_index;
mod btree_index;

pub use types::TypedValue;
pub use collection::CollectionManager;
pub use index::Index;
pub use hash_index::HashIndex;
pub use btree_index::BTreeIndex;

/// PyCQEngine Rust module - High-performance indexing for Python collections
#[pymodule]
fn _rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<CollectionManager>()?;
    Ok(())
}
