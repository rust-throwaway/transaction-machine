use crate::client::ClientState;
use crate::db::{StoreEngine, StoreError};
use crate::transaction::Transaction;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub const CLIENTS_KS: &str = "clients";
pub const TRANSACTIONS_KS: &str = "transactions";

/// A store for a client to lookup transactions, store transactions and persist its state.
#[derive(Debug, Clone)]
pub struct ClientStore<D>
where
    D: StoreEngine,
{
    /// The store to delegate operations to.
    delegate: Arc<D>,
}

impl<D> ClientStore<D>
where
    D: StoreEngine,
{
    /// Construct a new `ClientStore` that will delegate operations to `delegate`.
    pub fn new(delegate: D) -> Self {
        ClientStore {
            delegate: Arc::new(delegate),
        }
    }

    /// Returns a reference to this store's delegate engine.
    pub fn inner(&self) -> Arc<D> {
        self.delegate.clone()
    }
}

/// Keyspaces (column families in RocksDB).
pub enum Keyspace {
    Clients,
    Transactions,
}

impl Keyspace {
    pub fn name(&self) -> &str {
        match self {
            Keyspace::Clients => CLIENTS_KS,
            Keyspace::Transactions => TRANSACTIONS_KS,
        }
    }
}

fn serialize<S>(obj: &S) -> Result<Vec<u8>, StoreError>
where
    S: Serialize,
{
    bincode::serialize(obj).map_err(|e| StoreError::Serialize(Box::new(e)))
}

pub fn deserialize<'de, S>(obj: &'de [u8]) -> Result<S, StoreError>
where
    S: Deserialize<'de>,
{
    bincode::deserialize(obj).map_err(|e| StoreError::Deserialize(Box::new(e)))
}

impl<D> ClientStore<D>
where
    D: StoreEngine,
{
    /// Lookup a transaction in the store by `transaction_id`.
    pub fn get_transaction(&self, transaction_id: u32) -> Result<Option<Transaction>, StoreError> {
        let serialized_key = serialize(&transaction_id)?;
        match self
            .delegate
            .get(Keyspace::Transactions, serialized_key.as_slice())
        {
            Ok(Some(value)) => {
                let transaction = deserialize::<Transaction>(value.as_slice())?;
                Ok(Some(transaction))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Insert or update `transaction`.
    pub fn put_transaction(&self, transaction: Transaction) -> Result<(), StoreError> {
        let serialized_key = serialize(&transaction.id())?;
        let serialized_transaction = serialize(&transaction)?;

        self.delegate.put(
            Keyspace::Transactions,
            serialized_key.as_slice(),
            serialized_transaction.as_slice(),
        )
    }

    /// Lookup a client's state in the store by `client_id`.
    pub fn get_client_state(&self, client_id: u16) -> Result<Option<ClientState>, StoreError> {
        let serialized_key = serialize(&client_id)?;

        match self
            .delegate
            .get(Keyspace::Clients, serialized_key.as_slice())
        {
            Ok(Some(value)) => {
                let state = deserialize::<ClientState>(value.as_slice())?;
                Ok(Some(state))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Insert or update `state`.
    pub fn put_client_state(&self, state: &ClientState) -> Result<(), StoreError> {
        let serialized_key = serialize(&state.id())?;
        let serialized_client = serialize(&state)?;

        self.delegate.put(
            Keyspace::Clients,
            serialized_key.as_slice(),
            serialized_client.as_slice(),
        )
    }
}
