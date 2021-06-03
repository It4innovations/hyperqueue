pub use id_counter::IdCounter;
//pub use key_id_map::{Identifiable, KeyIdMap};
pub use wrapped::WrappedRcRefCell;

pub type Map<K, V> = hashbrown::HashMap<K, V>;
pub type Set<T> = hashbrown::HashSet<T>;

pub mod data;
pub mod error;
mod id_counter;
pub mod rpc;
mod wrapped;
#[macro_use]
pub mod trace;
pub mod resources;
pub mod secret;
pub mod setup;
