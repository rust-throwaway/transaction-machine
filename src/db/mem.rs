use crate::client::Keyspace;
use crate::db::{StoreEngine, StoreError};
use fnv::FnvHashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, RwLock};
use thiserror::Error;

type KeyspaceMap = FnvHashMap<Vec<u8>, Vec<u8>>;
type MemStoreMap = FnvHashMap<String, KeyspaceMap>;

/// An in-memory database.
#[derive(Debug, Clone, Default)]
pub struct MemStore {
    keyspaces: Arc<RwLock<MemStoreMap>>,
}

impl MemStore {
    /// Constructs a new memory store with the initial values of `keyspaces`.
    pub fn new(keyspaces: MemStoreMap) -> MemStore {
        MemStore {
            keyspaces: Arc::new(RwLock::new(keyspaces)),
        }
    }
}

#[derive(Debug, Error)]
#[error("Mutex poisoned")]
struct Poisoned;

impl StoreEngine for MemStore {
    fn put(&self, keyspace: Keyspace, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        let mut guard = self
            .keyspaces
            .write()
            .map_err(|_| StoreError::Write(Box::new(Poisoned)))?;
        let map = &mut *guard;
        let keyspace = keyspace.name().to_string();

        match map.entry(keyspace) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().insert(key.to_vec(), value.to_vec());
            }
            Entry::Vacant(entry) => {
                let mut inner = FnvHashMap::default();
                inner.insert(key.to_vec(), value.to_vec());
                entry.insert(inner);
            }
        }

        Ok(())
    }

    fn get(&self, keyspace: Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        let guard = self
            .keyspaces
            .read()
            .map_err(|_| StoreError::Read(Box::new(Poisoned)))?;
        let map = &*guard;

        let value = map
            .get(&keyspace.name().to_string())
            .and_then(|e| e.get(key).cloned());

        Ok(value)
    }
}
