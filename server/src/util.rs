// util.rs
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

pub type FileUid = String;
pub type KeyslotId = i16;

/// Calculates a consistent hash for the given UID.
pub fn hash(uid: &FileUid) -> usize {
    let mut hasher = DefaultHasher::new();
    uid.hash(&mut hasher);
    hasher.finish() as usize
}
