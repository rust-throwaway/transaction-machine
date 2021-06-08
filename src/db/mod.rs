#[cfg(test)]
mod tests;

#[cfg(feature = "rocks")]
mod disk;
#[cfg(feature = "rocks")]
pub use crate::db::disk::DiskStore;

mod mem;
pub use crate::db::mem::{MemStore, Poisoned};

use crate::client::Keyspace;
use std::error::Error;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("An error was produced when reading from the store: `{0}`")]
    Read(Box<dyn Error + Send>),
    #[error("An error was produced when reading writing to the store: `{0}`")]
    Write(Box<dyn Error + Send>),
    #[error("An error was produced when serializing a value: `{0}`")]
    Serialize(Box<dyn Error + Send>),
    #[error("An error was produced when deserializing a value: `{0}`")]
    Deserialize(Box<dyn Error + Send>),
    #[error("The requested keyspace was not found")]
    KeyspaceNotFound,
}

impl PartialEq for StoreError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (StoreError::Read(left), StoreError::Read(right)) => {
                left.to_string().eq(&right.to_string())
            }
            (StoreError::Write(left), StoreError::Write(right)) => {
                left.to_string().eq(&right.to_string())
            }
            (StoreError::KeyspaceNotFound, StoreError::KeyspaceNotFound) => true,
            _ => false,
        }
    }
}

/// An abstraction over a database store.
pub trait StoreEngine: Clone + Send + Sync {
    /// Attempt to put the key-value pair in to `keyspace`.
    fn put(&self, keyspace: Keyspace, key: &[u8], value: &[u8]) -> Result<(), StoreError>;

    /// Attempt to get `key` from the keyspace `keyspace`.
    fn get(&self, keyspace: Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError>;
}
