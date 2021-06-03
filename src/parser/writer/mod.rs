use crate::client::{deserialize, ClientState, ClientStore, Keyspace};
use crate::db::{DiskStore, StoreError};
use rocksdb::IteratorMode;
use serde::Serialize;
use std::fmt::{Display, Formatter};

/// Queries `store`'s `Clients` keyspace. Deserializing every client state record and printing it
/// to the standard output.
pub fn write_state(store: ClientStore<DiskStore>) -> Result<(), StoreError> {
    let inner = store.inner().delegate();
    let cf_handle = inner
        .cf_handle(Keyspace::Clients.name())
        .ok_or(StoreError::KeyspaceNotFound)?;
    let mut it = inner.iterator_cf(cf_handle, IteratorMode::Start);

    write_headers();

    loop {
        if it.valid() {
            match it.next() {
                Some((_key, value)) => {
                    let state = deserialize::<ClientState>(value.as_ref())?;
                    let state = State::from(state);
                    println!("{}", state);
                }
                None => break,
            }
        } else {
            let err = it.status().unwrap_err();
            return Err(StoreError::Deserialize(Box::new(err)));
        }
    }

    Ok(())
}

fn write_headers() {
    println!("client,\tavailable,\theld,\ttotal,\tlocked");
}

#[derive(Serialize, Debug)]
struct State {
    client: u16,
    available: f64,
    held: f64,
    total: f64,
    locked: bool,
}

impl Display for State {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let State {
            client,
            available,
            held,
            total,
            locked,
        } = self;

        write!(
            f,
            "{}, {:.4}, {:.4}, {:.4}, {}",
            client, available, held, total, locked
        )
    }
}

impl From<ClientState> for State {
    fn from(client: ClientState) -> Self {
        let (id, balance, frozen) = client.split();
        let available = balance.get_available();
        let held = balance.get_frozen();
        let total = balance.get_total();

        State {
            client: id,
            available,
            held,
            total,
            locked: frozen,
        }
    }
}
