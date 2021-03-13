pub use cycle_wrapped::{CycleOwner, HasCycle};
pub use id_counter::IdCounter;
//pub use key_id_map::{Identifiable, KeyIdMap};
pub use wrapped::WrappedRcRefCell;

pub type Map<K, V> = hashbrown::HashMap<K, V>;
pub type Set<T> = hashbrown::HashSet<T>;

mod cycle_wrapped;
pub mod data;
mod id_counter;
pub mod rpc;
mod wrapped;
pub mod error;
#[macro_use]
pub mod trace;
