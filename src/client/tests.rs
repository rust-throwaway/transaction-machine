use crate::client::store::{ClientStore, TRANSACTIONS_KS};
use crate::client::{
    ClientError, ClientState, ALREADY_DISPUTED, DISPUTE_MISMATCH, DISPUTE_WITHDRAWAL,
};
use crate::db::MemStore;
use crate::transaction::Transaction;
use bincode::serialize;
use fnv::FnvHashMap;

fn store() -> ClientStore<MemStore> {
    ClientStore::new(MemStore::default())
}

#[test]
fn deposit() {
    let mut client = ClientState::new(1);
    let result = client.execute_transaction(Transaction::deposit(1, 1, 100.0), &store());

    assert!(result.is_ok());
    assert_eq!(client.balance.get_available(), 100.0);
}

#[test]
fn invalid_client() {
    let mut client = ClientState::new(1);
    let result = client.execute_transaction(Transaction::deposit(2, 1, 100.0), &store());
    assert_eq!(result, Err(ClientError::MismatchedClientId));
}

#[test]
fn negative_deposit() {
    let mut client = ClientState::new(1);
    let result = client.execute_transaction(Transaction::deposit(1, 1, -100.0), &store());
    assert_eq!(result, Err(ClientError::NegativeValue));
}

#[test]
fn withdraw() {
    let mut client = ClientState::new(1);
    let deposit_result = client.execute_transaction(Transaction::deposit(1, 1, 100.0), &store());

    assert!(deposit_result.is_ok());
    assert_eq!(client.balance.get_available(), 100.0);

    let withdraw_result = client.execute_transaction(Transaction::withdrawal(1, 1, 50.0), &store());
    assert!(withdraw_result.is_ok());
    assert_eq!(client.balance.get_available(), 50.0);
}

#[test]
fn negative_withdraw() {
    let mut client = ClientState::new(1);
    let result = client.execute_transaction(Transaction::withdrawal(1, 1, -100.0), &store());
    assert_eq!(result, Err(ClientError::NegativeValue));
}

#[test]
fn insufficient_funds() {
    let mut client = ClientState::new(1);
    let result = client.execute_transaction(Transaction::withdrawal(1, 1, 100.0), &store());
    assert_eq!(result, Err(ClientError::InsufficientFunds));
}

#[test]
fn insufficient_funds_after_deposit() {
    let mut client = ClientState::new(1);

    let deposit_result = client.execute_transaction(Transaction::deposit(1, 1, 50.0), &store());
    assert!(deposit_result.is_ok());
    assert_eq!(client.balance.get_available(), 50.0);
    assert_eq!(client.balance.get_frozen(), 0.0);

    let result = client.execute_transaction(Transaction::withdrawal(1, 1, 100.0), &store());
    assert_eq!(result, Err(ClientError::InsufficientFunds));
}

#[test]
fn locked_account() {
    let mut client = ClientState::new(1);
    client.frozen = true;

    let result = client.execute_transaction(Transaction::resolve(1, 1), &store());
    assert_eq!(result, Err(ClientError::AccountFrozen));
}

fn load_store(txs: Vec<Transaction>) -> ClientStore<MemStore> {
    let mut keyspaces = FnvHashMap::default();
    let mut inner = FnvHashMap::default();

    for tx in txs {
        inner.insert(serialize(&tx.id()).unwrap(), serialize(&tx).unwrap());
    }
    keyspaces.insert(TRANSACTIONS_KS.to_string(), inner);

    ClientStore::new(MemStore::new(keyspaces))
}

#[test]
fn dispute_single() {
    let store = load_store(vec![Transaction::deposit(1, 1, 1.0)]);
    let mut client = ClientState::new(1);

    let deposit_result = client.execute_transaction(Transaction::deposit(1, 1, 1.0), &store);
    assert!(deposit_result.is_ok());
    assert_eq!(client.balance.get_available(), 1.0);

    let dispute_result = client.execute_transaction(Transaction::dispute(1, 1), &store);
    assert!(dispute_result.is_ok());
    assert_eq!(client.balance.get_available(), 0.0);
    assert_eq!(client.balance.get_frozen(), 1.0);

    assert_store_client(&store, &client);
}

#[test]
fn dispute_multiple() {
    let mut total = 0.0;
    let transactions = (1..=5)
        .into_iter()
        .map(|i| {
            let amount = i as f64 * 10.0;
            total += amount;
            Transaction::deposit(1, i, amount)
        })
        .collect::<Vec<_>>();

    let store = load_store(transactions.clone());
    let mut client = ClientState::new(1);

    for tx in transactions {
        let deposit_result = client.execute_transaction(tx, &store);
        assert!(deposit_result.is_ok());
    }

    assert_eq!(client.balance.get_available(), total);

    let dispute_result = client.execute_transaction(Transaction::dispute(1, 3), &store);
    assert!(dispute_result.is_ok());

    assert_eq!(client.balance.get_available(), total - 30.0);
    assert_eq!(client.balance.get_frozen(), 30.0);

    let dispute_result = client.execute_transaction(Transaction::dispute(1, 5), &store);
    assert!(dispute_result.is_ok());

    assert_eq!(client.balance.get_available(), total - 80.0);
    assert_eq!(client.balance.get_frozen(), 80.0);

    assert_store_client(&store, &client);
}

#[test]
fn dispute_unknown() {
    let mut client = ClientState::new(1);
    let dispute_result = client.execute_transaction(Transaction::dispute(1, 5), &store());
    assert_eq!(dispute_result, Err(ClientError::TransactionNotFound));
}

#[test]
fn dispute_dispute() {
    let mut client = ClientState::new(1);
    let store = load_store(vec![Transaction::dispute(1, 1)]);

    let dispute_result = client.execute_transaction(Transaction::dispute(1, 1), &store);
    assert_eq!(
        dispute_result,
        Err(ClientError::DisputeError(DISPUTE_MISMATCH.to_string()))
    );
}

#[test]
fn dispute_withdrawal() {
    let mut client = ClientState::new(1);

    let transactions = vec![
        Transaction::deposit(1, 1, 10.0),
        Transaction::withdrawal(1, 2, 5.0),
    ];

    let store = load_store(transactions.clone());

    for tx in transactions {
        let exec_result = client.execute_transaction(tx, &store);
        assert!(exec_result.is_ok());
    }

    assert_eq!(client.balance.get_available(), 5.0);
    assert_eq!(client.balance.get_frozen(), 0.0);

    let dispute_result = client.execute_transaction(Transaction::dispute(1, 2), &store);
    assert_eq!(
        dispute_result,
        Err(ClientError::DisputeError(DISPUTE_WITHDRAWAL.to_string()))
    );

    assert_store_client(&store, &client);
}

#[test]
fn dispute_then_deposit() {
    let transactions = vec![
        Transaction::deposit(1, 1, 10.0),
        Transaction::withdrawal(1, 2, 5.0),
    ];

    let store = load_store(transactions.clone());
    let mut client = ClientState::new(1);

    for tx in transactions {
        let exec_result = client.execute_transaction(tx, &store);
        assert!(exec_result.is_ok());
    }

    assert_eq!(client.balance.get_available(), 5.0);
    assert_eq!(client.balance.get_frozen(), 0.0);

    let dispute_result = client.execute_transaction(Transaction::dispute(1, 1), &store);
    assert!(dispute_result.is_ok());
    assert_disputed(1, &store);
    assert_eq!(client.balance.get_available(), -5.0);
    assert_eq!(client.balance.get_frozen(), 10.0);

    let deposit_result = client.execute_transaction(Transaction::deposit(1, 2, 10.0), &store);
    assert!(deposit_result.is_ok());
    assert_eq!(client.balance.get_available(), 5.0);
    assert_eq!(client.balance.get_frozen(), 10.0);

    assert_store_client(&store, &client);
}

fn assert_disputed(tx_id: u32, store: &ClientStore<MemStore>) {
    match store.get_transaction(tx_id) {
        Ok(Some(tx)) => match tx {
            Transaction::Transfer(e) if e.is_disputed() => {}
            t => panic!("Expected a disputed deposit transaction. Found `{:?}`", t),
        },
        Ok(None) => {
            panic!("Missing transaction")
        }
        Err(e) => {
            panic!("{:?}", e)
        }
    }
}

fn assert_not_disputed(tx_id: u32, store: &ClientStore<MemStore>) {
    match store.get_transaction(tx_id) {
        Ok(Some(tx)) => match tx {
            Transaction::Transfer(e) if !e.is_disputed() => {}
            t => panic!(
                "Expected an undisputed deposit transaction. Found `{:?}`",
                t
            ),
        },
        Ok(None) => {
            panic!("Missing transaction")
        }
        Err(e) => {
            panic!("{:?}", e)
        }
    }
}

fn assert_resolved(tx_id: u32, store: &ClientStore<MemStore>) {
    match store.get_transaction(tx_id) {
        Ok(Some(tx)) => match tx {
            Transaction::Transfer(e) if e.is_resolved() => {}
            t => panic!("Expected an resolved deposit transaction. Found `{:?}`", t),
        },
        Ok(None) => {
            panic!("Missing transaction")
        }
        Err(e) => {
            panic!("{:?}", e)
        }
    }
}

#[test]
fn dispute_negative() {
    let transactions = vec![
        Transaction::deposit(1, 1, 10.0),
        Transaction::withdrawal(1, 2, 10.0),
    ];

    let store = load_store(transactions.clone());
    let mut client = ClientState::new(1);

    for tx in transactions {
        let exec_result = client.execute_transaction(tx, &store);
        assert!(exec_result.is_ok());
    }

    assert_eq!(client.balance.get_available(), 0.0);
    assert_eq!(client.balance.get_frozen(), 0.0);

    let dispute_result = client.execute_transaction(Transaction::dispute(1, 1), &store);
    assert!(dispute_result.is_ok());
    assert_disputed(1, &store);

    assert_eq!(client.balance.get_available(), -10.0);
    assert_eq!(client.balance.get_frozen(), 10.0);

    let dispute_result = client.execute_transaction(Transaction::withdrawal(1, 3, 10.0), &store);
    assert_eq!(dispute_result, Err(ClientError::InsufficientFunds));

    assert_store_client(&store, &client);
}

#[test]
fn resolve_dispute() {
    let store = load_store(vec![Transaction::deposit(1, 1, 10.0)]);
    let mut client = ClientState::new(1);

    let exec_result = client.execute_transaction(Transaction::deposit(1, 1, 10.0), &store);
    assert!(exec_result.is_ok());
    assert_eq!(client.balance.get_available(), 10.0);
    assert_eq!(client.balance.get_frozen(), 0.0);

    let dispute_result = client.execute_transaction(Transaction::dispute(1, 1), &store);
    assert!(dispute_result.is_ok());
    assert_disputed(1, &store);
    assert_eq!(client.balance.get_available(), 0.0);
    assert_eq!(client.balance.get_frozen(), 10.0);

    let dispute_result = client.execute_transaction(Transaction::resolve(1, 1), &store);
    assert!(dispute_result.is_ok());
    assert_not_disputed(1, &store);

    assert_eq!(client.balance.get_available(), 10.0);
    assert_eq!(client.balance.get_frozen(), 0.0);

    assert_store_client(&store, &client);
}

#[test]
fn dispute_withdrawn_funds() {
    let transactions = vec![
        Transaction::deposit(1, 1, 10.0),
        Transaction::withdrawal(1, 2, 10.0),
    ];

    let store = load_store(transactions.clone());
    let mut client = ClientState::new(1);

    for tx in transactions {
        let exec_result = client.execute_transaction(tx, &store);
        assert!(exec_result.is_ok());
    }

    assert_eq!(client.balance.get_available(), 0.0);
    assert_eq!(client.balance.get_frozen(), 0.0);

    let dispute_result = client.execute_transaction(Transaction::dispute(1, 1), &store);
    assert!(dispute_result.is_ok());
    assert_disputed(1, &store);
    assert_eq!(client.balance.get_available(), -10.0);
    assert_eq!(client.balance.get_frozen(), 10.0);

    let deposit_result = client.execute_transaction(Transaction::deposit(1, 3, 10.0), &store);
    assert!(deposit_result.is_ok());
    assert_eq!(client.balance.get_available(), 0.0);
    assert_eq!(client.balance.get_frozen(), 10.0);

    let dispute_result = client.execute_transaction(Transaction::resolve(1, 1), &store);
    assert!(dispute_result.is_ok());
    assert_not_disputed(1, &store);
    assert_eq!(client.balance.get_available(), 10.0);
    assert_eq!(client.balance.get_frozen(), 0.0);

    assert_store_client(&store, &client);
}

#[test]
fn chargeback() {
    let store = load_store(vec![Transaction::deposit(1, 1, 10.0)]);
    let mut client = ClientState::new(1);

    let exec_result = client.execute_transaction(Transaction::deposit(1, 1, 10.0), &store);
    assert!(exec_result.is_ok());

    assert_eq!(client.balance.get_available(), 10.0);
    assert_eq!(client.balance.get_frozen(), 0.0);

    let dispute_result = client.execute_transaction(Transaction::dispute(1, 1), &store);
    assert!(dispute_result.is_ok());
    assert_disputed(1, &store);
    assert_eq!(client.balance.get_available(), 0.0);
    assert_eq!(client.balance.get_frozen(), 10.0);

    let chargeback_result = client.execute_transaction(Transaction::chargeback(1, 1), &store);
    assert!(chargeback_result.is_ok());
    assert_resolved(1, &store);
    assert_eq!(client.balance.get_available(), 0.0);
    assert_eq!(client.balance.get_frozen(), 0.0);
    assert!(client.frozen);

    let dispute_result = client.execute_transaction(Transaction::deposit(1, 1, 10.0), &store);
    assert_eq!(dispute_result, Err(ClientError::AccountFrozen));

    assert_store_client(&store, &client);
}

#[test]
fn double_dispute() {
    let store = load_store(vec![Transaction::deposit(1, 1, 10.0)]);
    let mut client = ClientState::new(1);

    let exec_result = client.execute_transaction(Transaction::deposit(1, 1, 10.0), &store);
    assert!(exec_result.is_ok());

    assert_eq!(client.balance.get_available(), 10.0);
    assert_eq!(client.balance.get_frozen(), 0.0);

    let dispute_result = client.execute_transaction(Transaction::dispute(1, 1), &store);
    assert!(dispute_result.is_ok());
    assert_disputed(1, &store);
    assert_eq!(client.balance.get_available(), 0.0);
    assert_eq!(client.balance.get_frozen(), 10.0);

    let dispute_result = client.execute_transaction(Transaction::dispute(1, 1), &store);
    assert_eq!(
        dispute_result,
        Err(ClientError::DisputeError(ALREADY_DISPUTED.to_string()))
    );

    assert_store_client(&store, &client);
}

fn assert_store_client(store: &ClientStore<MemStore>, expected: &ClientState) {
    match store.get_client_state(expected.id) {
        Ok(Some(store_client)) => {
            assert_eq!(expected, &store_client)
        }
        r => panic!("Expected a client, found: `{:?}`", r),
    }
}

#[test]
fn store_updates() {
    let store = load_store(vec![Transaction::deposit(1, 1, 10.0)]);
    let mut client = ClientState::new(1);

    let exec_result = client.execute_transaction(Transaction::deposit(1, 1, 10.0), &store);
    assert!(exec_result.is_ok());

    assert_store_client(&store, &client);
}
