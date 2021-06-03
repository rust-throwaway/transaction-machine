use crate::client::{Client, ClientError, ClientRequest, ClientStore};
use crate::db::{StoreEngine, StoreError};
use crate::transaction::Transaction;
use futures::StreamExt;
use lru::LruCache;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{span, Level};
use tracing_futures::Instrument;

const MAX_CLIENTS: usize = 2048;
const CLIENT_TASK: &str = "Client task";
const CLIENT_STOPPED: &str = "Client stopped unexpectedly";
const NO_RESPONSE: &str = "No response received from client";

/// A handle to a client that is currently running.
#[derive(Clone)]
struct ClientHandle {
    _handle: Arc<JoinHandle<()>>,
    /// A sender for forwarding transactions to.
    tx: mpsc::Sender<ClientRequest>,
}

impl ClientHandle {
    /// Initialises a new client instance from `store` if it has previously been run. Or constructs
    /// a new client instance. Returns either a handle that can be used to forward transactions to
    /// or an initialisation error.
    fn new<D>(
        id: u16,
        store: ClientStore<D>,
        channel_size: usize,
    ) -> Result<ClientHandle, StoreError>
    where
        D: StoreEngine + 'static,
    {
        let (tx, rx) = mpsc::channel(channel_size);

        let state = store.get_client_state(id)?;
        let client = match state {
            Some(previous_state) => Client::with_state(previous_state, rx, store),
            None => Client::new(id, rx, store),
        };

        let task = tokio::spawn(async move {
            client
                .run()
                .instrument(span!(Level::INFO, CLIENT_TASK, ?id))
                .await;
        });

        Ok(ClientHandle {
            _handle: Arc::new(task),
            tx,
        })
    }

    /// Executes `transaction` against this client handle. Returning the result of the execution.
    async fn execute_transaction(&self, transaction: Transaction) -> Result<(), ClientError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ClientRequest {
                transaction,
                callback: tx,
            })
            .await
            .expect(CLIENT_STOPPED);
        rx.await.expect(NO_RESPONSE)
    }
}

/// An IO task between a reader (`rx`) and clients.
pub struct IoTask<D>
where
    D: StoreEngine,
{
    rx: mpsc::Receiver<Transaction>,
    store: ClientStore<D>,
}

impl<D> IoTask<D>
where
    D: StoreEngine,
{
    /// Constructs a new IO task that will listen on `rx`, run clients on demand and execute all
    /// transactions that are received.
    pub fn new(rx: mpsc::Receiver<Transaction>, store: ClientStore<D>) -> Self {
        IoTask { rx, store }
    }
}

impl<D> IoTask<D>
where
    D: StoreEngine + 'static,
{
    /// Runs this IO task until completion or an error is produced. Any fatal error (a store error)
    /// will cause this task to terminate.
    ///
    /// Any event that is received will be forwarded to either a new client instance that is
    /// restored from its previous state if it exists, or a new instance is constructed.
    ///
    /// Running clients are stored in an LRU cache to reduce the memory footprint of this
    /// application and to not keep old clients running.
    pub async fn run(self, channel_size: usize) -> Result<(), StoreError> {
        let IoTask { rx, store } = self;

        let mut clients: LruCache<u16, ClientHandle> = LruCache::new(MAX_CLIENTS);
        let mut requests = ReceiverStream::new(rx);

        while let Some(transaction) = requests.next().await {
            match clients.get(&transaction.client_id()) {
                Some(handle) => {
                    let result = handle.execute_transaction(transaction).await;
                    on_result(result);
                }
                None => {
                    let client_id = transaction.client_id();
                    let handle = ClientHandle::new(client_id, store.clone(), channel_size)?;
                    let result = handle.execute_transaction(transaction).await;
                    on_result(result);

                    let _removed = clients.put(client_id, handle);
                }
            };
        }

        Ok(())
    }
}

fn on_result(result: Result<(), ClientError>) {
    if let Err(e) = result {
        if e.is_fatal() {
            panic!("Client fatally errored with `{:?}`", e);
        }
    }
}
