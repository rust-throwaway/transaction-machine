use crate::parser::CsvTransaction;
use crate::transaction::Transaction;
use csv::{Reader, ReaderBuilder, Trim};
use std::convert::TryFrom;
use std::io::Read;

fn reader<R: Read>(reader: R) -> Reader<R> {
    ReaderBuilder::new()
        .trim(Trim::All)
        .flexible(true)
        .has_headers(true)
        .from_reader(reader)
}

fn read_single(input: &str, expected: Transaction) {
    let mut reader = reader(input.as_bytes()).into_deserialize::<CsvTransaction>();
    let record = reader.next().expect("Missing record");

    match record {
        Ok(csv_transaction) => {
            let tx = Transaction::try_from(csv_transaction).unwrap();
            assert_eq!(tx, expected);
        }
        Err(e) => panic!("{:?}", e),
    }
}

fn read_multiple(input: &str, expected: Vec<Transaction>) {
    let reader = reader(input.as_bytes()).into_deserialize::<CsvTransaction>();
    let records = reader
        .map(|record| match record {
            Ok(csv_transaction) => Transaction::try_from(csv_transaction).unwrap(),
            Err(e) => panic!("{:?}", e),
        })
        .collect::<Vec<_>>();

    assert_eq!(records, expected);
}

fn read_err(input: &str) {
    let mut reader = reader(input.as_bytes()).into_deserialize::<CsvTransaction>();
    let record = reader.next().expect("Missing record");

    if let Ok(rec) = record {
        panic!("Expected an error. Got `{:?}`", rec)
    }
}

#[test]
fn withdrawal() {
    let input = "type, client,  tx,amount
withdrawal, 1,   1,  1.0";

    read_single(input, Transaction::withdrawal(1, 1, 1.0));
}

#[test]
fn deposit() {
    let input = "type, client,  tx,amount
deposit, 1,   1,  1.0";

    read_single(input, Transaction::deposit(1, 1, 1.0));
}

#[test]
fn dispute() {
    let input = "type, client,  tx,amount
dispute, 1,   1";

    read_single(input, Transaction::dispute(1, 1));
}

#[test]
fn resolve() {
    let input = "type, client,  tx,amount
resolve, 1,   1";

    read_single(input, Transaction::resolve(1, 1));
}

#[test]
fn chargeback() {
    let input = "type, client,  tx,amount
chargeback, 1,   1";

    read_single(input, Transaction::chargeback(1, 1));
}

#[test]
fn unknown() {
    let input = "type, client,  tx,amount
buy, 1,   1";
    read_err(input)
}

#[test]
fn no_whitespace() {
    let input = "type,client,tx,amount
chargeback,1,1";

    read_single(input, Transaction::chargeback(1, 1));
}

#[test]
fn whitespace() {
    let input = "type  ,       client     , tx ,   amount
chargeback        ,        1        , 1 ";

    read_single(input, Transaction::chargeback(1, 1));
}

#[test]
fn casing() {
    let input = "type  ,       client     , tx ,   amount
CHARGEBACK        ,        1        , 1 ";
    read_err(input);
}

#[test]
fn invalid_type() {
    let input = "type,client,tx,amount
chargeback,1.0,1 ";
    read_err(input);
}

#[test]
fn integer_amount() {
    let input = "type, client,  tx,amount
deposit, 1,   1,  1";

    read_single(input, Transaction::deposit(1, 1, 1.0));
}

#[test]
fn high_precision() {
    let input = "type, client,  tx,amount
deposit, 1,   1,  1.23456789";

    read_single(input, Transaction::deposit(1, 1, 1.23456789));
}

#[test]
fn multiple() {
    let input = "type, client,  tx,amount
withdrawal, 1,   1,  1.0
deposit, 1,   1,  1.0
dispute, 1,   1
resolve, 1,   1
chargeback, 1,   1";

    let expected = vec![
        Transaction::withdrawal(1, 1, 1.0),
        Transaction::deposit(1, 1, 1.0),
        Transaction::dispute(1, 1),
        Transaction::resolve(1, 1),
        Transaction::chargeback(1, 1),
    ];

    read_multiple(input, expected);
}
