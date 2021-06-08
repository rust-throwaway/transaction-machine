use crate::client::{ClientState, ClientStore, Keyspace};
use crate::db::MemStore;
use crate::parser::CsvTransaction;
use crate::transaction::Transaction;
use csv::Writer;
use fnv::FnvHashMap;
use rand::Rng;
use std::collections::HashMap;
use std::convert::TryFrom;

const OUT_FILE_NAME: &str = "generated.csv";

/// Produces an empty memory store initialised with the default keyspaces
pub fn mem_store() -> ClientStore<MemStore> {
    let mut keyspaces = FnvHashMap::default();
    let transactions = FnvHashMap::default();
    let clients = FnvHashMap::default();

    keyspaces.insert(Keyspace::Transactions.name().to_string(), transactions);
    keyspaces.insert(Keyspace::Clients.name().to_string(), clients);

    ClientStore::new(MemStore::new(keyspaces))
}

/// Generates `count` transactions and writes them to `generated.csv`.
pub fn generate_csv(count: usize) {
    let mut rng = rand::thread_rng();

    let mut states: HashMap<u16, ClientState> = HashMap::new();
    let mut transactions: Vec<Transaction> = Vec::new();
    let mut disputed_transactions = Vec::new();

    let store = mem_store();

    for i in 0..count {
        let dispute = rng.gen_bool(0.1);
        if dispute && !transactions.is_empty() {
            if disputed_transactions.is_empty() {
                let idx = rng.gen_range(0..transactions.len());
                let tx = &transactions[idx];
                let disputed = Transaction::dispute(tx.client_id(), tx.id());

                let client = states.get_mut(&disputed.client_id()).unwrap();
                let _ = client.execute_transaction(disputed.clone(), &store);

                disputed_transactions.push(disputed.clone());
                transactions.push(disputed)
            } else {
                let num = rng.gen_range(0..10);
                match num {
                    0..=5 => {
                        let idx = rng.gen_range(0..transactions.len());
                        let tx = &transactions[idx];
                        let disputed = Transaction::dispute(tx.client_id(), tx.id());

                        let client = states.get_mut(&disputed.client_id()).unwrap();
                        let _ = client.execute_transaction(disputed.clone(), &store);

                        disputed_transactions.push(disputed.clone());
                        transactions.push(disputed)
                    }
                    6..=8 => {
                        let idx = rng.gen_range(0..disputed_transactions.len());
                        let tx = &disputed_transactions[idx];
                        let disputed = Transaction::resolve(tx.client_id(), tx.id());

                        let client = states.get_mut(&disputed.client_id()).unwrap();
                        let _ = client.execute_transaction(disputed.clone(), &store);

                        disputed_transactions.remove(idx);
                        transactions.push(disputed)
                    }
                    _ => {
                        let idx = rng.gen_range(0..disputed_transactions.len());
                        let tx = &disputed_transactions[idx];
                        let disputed = Transaction::chargeback(tx.client_id(), tx.id());

                        let client = states.get_mut(&disputed.client_id()).unwrap();
                        let _ = client.execute_transaction(disputed.clone(), &store);

                        disputed_transactions.remove(idx);
                        transactions.push(disputed)
                    }
                }
            }
        } else {
            let len = states.len();
            let client = if u16::try_from(len + 1).is_ok() {
                let len = len as u16;

                let make_new_client = rng.gen_bool(0.3);
                if make_new_client || states.is_empty() {
                    let id = states.len() as u16;
                    let client = ClientState::new(id);
                    states.insert(id, client);
                    states.get_mut(&id).unwrap()
                } else {
                    let idx = rng.gen_range(0..len);
                    states.get_mut(&idx).unwrap()
                }
            } else {
                let idx = rng.gen_range(0..len as u16);
                states.get_mut(&idx).unwrap()
            };

            let withdrawal = rng.gen_bool(0.5);
            let amount = rng.gen_range(0.0..1000.0);
            if withdrawal {
                let tx = Transaction::withdrawal(client.id(), i as u32, amount);
                let _ = client.execute_transaction(tx.clone(), &store);
                transactions.push(tx);
            } else {
                let tx = Transaction::deposit(client.id(), i as u32, amount);
                let _ = client.execute_transaction(tx.clone(), &store);
                transactions.push(tx);
            }
        }
    }

    let mut wtr = Writer::from_path(OUT_FILE_NAME).unwrap();

    for tx in transactions {
        let csv = CsvTransaction::from(tx);
        wtr.serialize(csv).unwrap();
    }
}
