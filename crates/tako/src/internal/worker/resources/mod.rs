pub mod allocator;
pub mod concise;
pub mod map;
pub mod pool;

mod groups;
mod solver;

#[cfg(feature = "highs")]
pub(crate) mod solver_highs;

#[cfg(all(feature = "microlp", not(feature = "highs")))]
pub(crate) mod solver_microlp;

#[cfg(test)]
mod test_allocator;
