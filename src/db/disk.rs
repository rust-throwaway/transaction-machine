use crate::client::Keyspace;
use crate::db::{StoreEngine, StoreError};
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, Options, DB};
use std::path::Path;
use std::sync::Arc;

/// A persistent disk store which is backed by a Rocks Database.
#[derive(Debug, Clone)]
pub struct DiskStore {
    delegate: Arc<DB>,
}

impl DiskStore {
    /// Attempts to open a new `DiskStore` at the provided `path`.
    pub fn new<P>(path: P) -> Result<DiskStore, StoreError>
    where
        P: AsRef<Path>,
    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let clients = ColumnFamilyDescriptor::new(Keyspace::Clients.name(), Options::default());
        let transactions =
            ColumnFamilyDescriptor::new(Keyspace::Transactions.name(), Options::default());

        DB::open_cf_descriptors(&opts, path, vec![clients, transactions])
            .map(|db| DiskStore {
                delegate: Arc::new(db),
            })
            .map_err(|e| StoreError::InitialisationError(Box::new(e)))
    }

    pub fn delegate(&self) -> Arc<DB> {
        self.delegate.clone()
    }
}

fn resolve_keyspace(store: &Arc<DB>, keyspace: Keyspace) -> Result<&ColumnFamily, StoreError> {
    store
        .cf_handle(keyspace.name())
        .ok_or(StoreError::KeyspaceNotFound)
}

impl StoreEngine for DiskStore {
    fn put(&self, keyspace: Keyspace, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        let keyspace = resolve_keyspace(&self.delegate, keyspace)?;
        self.delegate
            .put_cf(keyspace, key, value)
            .map_err(|e| StoreError::Write(Box::new(e)))
    }

    fn get(&self, keyspace: Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        let keyspace = resolve_keyspace(&self.delegate, keyspace)?;
        match self.delegate.get_cf(keyspace, key) {
            Ok(Some(value)) => Ok(Some(value)),
            Ok(None) => Ok(None),
            Err(e) => Err(StoreError::Read(Box::new(e))),
        }
    }
}
