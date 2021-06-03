#[cfg(test)]
mod tests;

mod balance;
mod store;
pub use store::deserialize;
pub use store::Keyspace;

use crate::client::balance::{Account, UpdateError};
pub use crate::client::store::ClientStore;
use crate::db::{StoreEngine, StoreError};
use crate::transaction::{
    DisputeStatus, DisputedTransaction, DisputedTransactionKind, Transaction, TransferTransaction,
    TransferTransactionKind,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{event, Level};

const DISPUTE_MISMATCH: &str = "Only a transfer can be disputed";
const DISPUTE_WITHDRAWAL: &str = "Cannot dispute a withdrawal";
const NOT_DISPUTED: &str = "Transaction is not disputed";
const ALREADY_DISPUTED: &str = "Transaction is already disputed";
const DISPUTE_RESOLVED: &str = "Dispute already resolved";
const EXEC_TRANSACTION: &str = "Executing transaction";
const TRANSACTION_ERR: &str = "An error was produced when executing a transaction";

/// An asynchronous request for this client to execute the provided transaction.
#[derive(Debug)]
pub struct ClientRequest {
    /// The transaction to execute against this client.
    pub transaction: Transaction,
    /// A callback to provide the result of executing the transaction.
    pub callback: oneshot::Sender<Result<(), ClientError>>,
}

/// A client model for this transaction machine to execute transactions against.
#[derive(Debug)]
pub struct Client<D>
where
    D: StoreEngine,
{
    /// The internal state of this client - including its balance.
    state: ClientState,
    /// A channel to listen on to serve requests.
    rx: mpsc::Receiver<ClientRequest>,
    /// A delegate store for persisting this clients state and the transactions that it has
    /// processed.
    store: ClientStore<D>,
}

impl<D> Client<D>
where
    D: StoreEngine,
{
    /// Initialise a new client with a default state.
    pub fn new(id: u16, rx: mpsc::Receiver<ClientRequest>, store: ClientStore<D>) -> Self {
        Client {
            state: ClientState::new(id),
            rx,
            store,
        }
    }

    /// Load a new client with `state`.
    pub fn with_state(
        state: ClientState,
        rx: mpsc::Receiver<ClientRequest>,
        store: ClientStore<D>,
    ) -> Self {
        Client { state, rx, store }
    }

    /// Run this client asynchronously until its internal `rx` channel has no subscribers.
    pub async fn run(self) {
        let Client {
            mut state,
            rx,
            store,
            ..
        } = self;

        let mut requests = ReceiverStream::new(rx);
        while let Some(request) = requests.next().await {
            let ClientRequest {
                transaction,
                callback,
            } = request;

            let result = state.execute_transaction(transaction, &store);
            let _ = callback.send(result);
        }
    }
}

/// The internal state of a client.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ClientState {
    /// A unique identifier that has been assigned to this client.
    id: u16,
    /// This client's balance.
    balance: Account,
    /// Whether the client's account has been frozen and it should stop executing transactions.
    frozen: bool,
}

impl ClientState {
    /// Initialise a new `ClientState` with default values and `id`.
    pub fn new(id: u16) -> ClientState {
        ClientState {
            id,
            balance: Default::default(),
            frozen: false,
        }
    }

    /// Split this client in to its internal parts.
    pub fn split(self) -> (u16, Account, bool) {
        let ClientState {
            id,
            balance,
            frozen,
        } = self;
        (id, balance, frozen)
    }

    /// Returns the unique identifier assigned to this client.
    pub fn id(&self) -> u16 {
        self.id
    }

    /// Execute a `TransferTransaction` against this `ClientState`. If the operation is successful,
    /// then the result of the operation is persisted in `store`.
    fn execute_transfer<D>(
        &mut self,
        transaction: TransferTransaction,
        store: &ClientStore<D>,
    ) -> Result<(), ClientError>
    where
        D: StoreEngine,
    {
        let TransferTransaction { kind, amount, .. } = &transaction;
        let result = match kind {
            TransferTransactionKind::Deposit => self.balance.deposit(*amount).map_err(Into::into),
            TransferTransactionKind::Withdrawal => {
                self.balance.withdraw(*amount).map_err(Into::into)
            }
        };

        if result.is_ok() {
            store.put_transaction(Transaction::Transfer(transaction))?;
        }

        result
    }

    /// Execute a `DisputedTransaction` against this `ClientState`. If the operation is successful,
    /// then the result of the operation is persisted in `store`.
    fn execute_disputed_transaction<D>(
        &mut self,
        transaction: DisputedTransaction,
        store: &ClientStore<D>,
    ) -> Result<(), ClientError>
    where
        D: StoreEngine,
    {
        let DisputedTransaction { kind, tx, .. } = transaction;

        match kind {
            DisputedTransactionKind::Dispute => self.execute_dispute(tx, store),
            DisputedTransactionKind::Resolve => self.execute_resolve(tx, store),
            DisputedTransactionKind::Chargeback => self.execute_chargeback(tx, store),
        }
    }

    /// Attempts to dispute a transaction that this state object has previously processed. If the
    /// transaction has not been processed previously then an error is returned. The funds are
    /// disputed are moved to being in a held state.
    fn execute_dispute<D>(&mut self, tx_id: u32, store: &ClientStore<D>) -> Result<(), ClientError>
    where
        D: StoreEngine,
    {
        match store.get_transaction(tx_id)? {
            Some(Transaction::Transfer(transfer)) => {
                let TransferTransaction {
                    kind,
                    client,
                    tx,
                    amount,
                    disputed,
                } = transfer;
                match kind {
                    TransferTransactionKind::Deposit => {
                        if matches!(disputed, DisputeStatus::Disputed) {
                            return Err(ClientError::DisputeError(ALREADY_DISPUTED.to_string()));
                        }

                        let processed = TransferTransaction {
                            kind,
                            client,
                            tx,
                            amount,
                            disputed: DisputeStatus::Disputed,
                        };

                        store
                            .put_transaction(Transaction::Transfer(processed))
                            .map_err(ClientError::StoreError)?;

                        self.balance
                            .hold(amount)
                            .map_err::<ClientError, _>(Into::into)
                    }
                    TransferTransactionKind::Withdrawal => {
                        Err(ClientError::DisputeError(DISPUTE_WITHDRAWAL.to_string()))
                    }
                }
            }
            Some(Transaction::Disputed(_)) => {
                Err(ClientError::DisputeError(DISPUTE_MISMATCH.to_string()))
            }
            None => Err(ClientError::TransactionNotFound),
        }
    }

    /// Attempts to resolve a transaction that has previously been marked as disputed. If the
    /// corresponding transaction does not exist then an error is returned. Any held funds are
    /// released if the operation is successful.
    fn execute_resolve<D>(&mut self, tx_id: u32, store: &ClientStore<D>) -> Result<(), ClientError>
    where
        D: StoreEngine,
    {
        match store.get_transaction(tx_id)? {
            Some(Transaction::Transfer(transfer)) => {
                let TransferTransaction {
                    kind,
                    client,
                    tx,
                    amount,
                    disputed,
                } = transfer;

                if disputed == DisputeStatus::NotDisputed {
                    return Err(ClientError::DisputeError(NOT_DISPUTED.to_string()));
                }

                let processed = TransferTransaction {
                    kind,
                    client,
                    tx,
                    amount,
                    disputed: DisputeStatus::NotDisputed,
                };

                store
                    .put_transaction(Transaction::Transfer(processed))
                    .map_err(ClientError::StoreError)?;

                self.balance.release(amount);
                Ok(())
            }
            Some(Transaction::Disputed(_)) => {
                Err(ClientError::DisputeError(DISPUTE_MISMATCH.to_string()))
            }
            None => Err(ClientError::TransactionNotFound),
        }
    }

    /// Executes a chargeback against this `ClientState` instance. If the corresponding transaction
    /// does not exist then an error is returned. Otherwise, the held funds are removed from this
    /// client.
    fn execute_chargeback<D>(
        &mut self,
        tx_id: u32,
        store: &ClientStore<D>,
    ) -> Result<(), ClientError>
    where
        D: StoreEngine,
    {
        match store.get_transaction(tx_id)? {
            Some(Transaction::Transfer(transfer)) => {
                let TransferTransaction {
                    kind,
                    client,
                    tx,
                    amount,
                    disputed,
                } = transfer;

                if matches!(disputed, DisputeStatus::NotDisputed) {
                    return Err(ClientError::DisputeError(NOT_DISPUTED.to_string()));
                } else if matches!(disputed, DisputeStatus::Resolved) {
                    return Err(ClientError::DisputeError(DISPUTE_RESOLVED.to_string()));
                }

                let processed = TransferTransaction {
                    kind,
                    client,
                    tx,
                    amount,
                    disputed: DisputeStatus::Resolved,
                };

                store
                    .put_transaction(Transaction::Transfer(processed))
                    .map_err(ClientError::StoreError)?;

                self.balance.charge(amount);
                self.frozen = true;

                Ok(())
            }
            Some(Transaction::Disputed(_)) => {
                Err(ClientError::DisputeError(DISPUTE_MISMATCH.to_string()))
            }
            None => Err(ClientError::TransactionNotFound),
        }
    }

    /// Executes `transaction` against this `ClientState`. If the operation is successful, then this
    /// `ClientState`'s updated state is persisted.
    pub fn execute_transaction<D>(
        &mut self,
        transaction: Transaction,
        store: &ClientStore<D>,
    ) -> Result<(), ClientError>
    where
        D: StoreEngine,
    {
        if self.id != transaction.client_id() {
            Err(ClientError::MismatchedClientId)
        } else if self.frozen {
            Err(ClientError::AccountFrozen)
        } else {
            event!(Level::TRACE, EXEC_TRANSACTION, ?transaction);

            let result = match transaction {
                Transaction::Transfer(tx) => self.execute_transfer(tx, store),
                Transaction::Disputed(tx) => self.execute_disputed_transaction(tx, store),
            };

            match result {
                Ok(()) => store
                    .put_client_state(self)
                    .map_err(ClientError::StoreError),
                Err(error) => {
                    event!(Level::ERROR, TRANSACTION_ERR, ?error);
                    Err(error)
                }
            }
        }
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum ClientError {
    #[error("Attempted to execute a transaction that contained a negative amount")]
    NegativeValue,
    #[error("The client has insufficient funds")]
    InsufficientFunds,
    #[error("Attempted to execute a transaction that was not for this client")]
    MismatchedClientId,
    #[error("Cannot execute a transaction against this client as its account is frozen")]
    AccountFrozen,
    #[error("A reference to a transaction was provided that does not exist")]
    TransactionNotFound,
    #[error("Dispute error: `{0}`")]
    DisputeError(String),
    #[error("Store error: `{0}`")]
    StoreError(StoreError),
}

impl ClientError {
    pub fn is_fatal(&self) -> bool {
        matches!(self, ClientError::StoreError(_))
    }
}

impl From<UpdateError> for ClientError {
    fn from(e: UpdateError) -> Self {
        match e {
            UpdateError::NegativeValue => ClientError::NegativeValue,
            UpdateError::InsufficientFunds => ClientError::InsufficientFunds,
        }
    }
}

impl From<StoreError> for ClientError {
    fn from(e: StoreError) -> Self {
        ClientError::StoreError(e)
    }
}
