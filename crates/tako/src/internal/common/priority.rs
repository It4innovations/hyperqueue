use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// User-defined priority
///
/// A larger number ==> a higher priority
#[derive(
    Default, Debug, Copy, Clone, Hash, PartialOrd, Ord, PartialEq, Eq, Serialize, Deserialize,
)]
pub struct UserPriority(i32);

impl UserPriority {
    pub fn new(value: i32) -> Self {
        Self(value)
    }
}

impl From<i32> for UserPriority {
    fn from(value: i32) -> Self {
        UserPriority::new(value)
    }
}

impl Display for UserPriority {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Main priority type that mixes user priority, scheduler priority, resource priority
///
/// A larger number ==> a higher priority
#[derive(
    Default, Debug, Copy, Clone, Hash, PartialOrd, Ord, PartialEq, Eq, Serialize, Deserialize,
)]
pub struct Priority(u64);

impl Priority {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn from_user_priority(user_priority: UserPriority) -> Self {
        Priority((user_priority.0 as u64 ^ 0x8000_0000) << 32)
    }

    pub fn new_resource_priority(value: u64) -> Self {
        Priority(value << 16)
    }

    pub fn remove_priority_u32(&self, value: u32) -> Priority {
        Priority(self.0.saturating_add((u32::MAX - value) as u64))
    }

    pub fn combine(self, other: Priority) -> Priority {
        Priority(self.0.saturating_add(other.0))
    }
}

impl From<u64> for Priority {
    fn from(value: u64) -> Self {
        Self(value)
    }
}
