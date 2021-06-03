pub mod reader;
pub mod writer;

use crate::transaction::{
    DisputedTransaction, DisputedTransactionKind, Transaction, TransactionType,
    TransferTransaction, TransferTransactionKind,
};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use thiserror::Error;

// The CSV crate doesn't work well with untagged enums. So this serves as an intermediary parsing
// step to get to and from the transaction structure.
#[derive(Serialize, Deserialize, Debug)]
pub struct CsvTransaction {
    #[serde(rename = "type")]
    tx_type: TransactionType,
    client: u16,
    tx: u32,
    amount: Option<f64>,
}

impl From<Transaction> for CsvTransaction {
    fn from(tx: Transaction) -> Self {
        match tx {
            Transaction::Transfer(tx) => {
                let TransferTransaction {
                    kind,
                    client,
                    tx,
                    amount,
                    ..
                } = tx;

                CsvTransaction {
                    tx_type: kind.into(),
                    client,
                    tx,
                    amount: Some(amount),
                }
            }
            Transaction::Disputed(tx) => {
                let DisputedTransaction { kind, client, tx } = tx;

                CsvTransaction {
                    tx_type: kind.into(),
                    client,
                    tx,
                    amount: None,
                }
            }
        }
    }
}

#[derive(Error, Debug)]
pub enum CsvParseError {
    #[error("Expected no amount to be provided")]
    ExpectedNoAmount,
    #[error("Expected an amount to be provided")]
    ExpectedAnAmount,
}

impl TryFrom<CsvTransaction> for Transaction {
    type Error = CsvParseError;

    fn try_from(tx: CsvTransaction) -> Result<Self, Self::Error> {
        let CsvTransaction {
            tx_type,
            client,
            tx,
            amount,
        } = tx;
        let tx = match tx_type {
            TransactionType::Deposit => TransferTransaction {
                kind: TransferTransactionKind::Deposit,
                client,
                tx,
                amount: amount.ok_or(CsvParseError::ExpectedAnAmount)?,
                disputed: Default::default(),
            }
            .into(),
            TransactionType::Withdrawal => TransferTransaction {
                kind: TransferTransactionKind::Withdrawal,
                client,
                tx,
                amount: amount.ok_or(CsvParseError::ExpectedAnAmount)?,
                disputed: Default::default(),
            }
            .into(),
            TransactionType::Dispute => match amount {
                Some(_) => return Err(CsvParseError::ExpectedNoAmount),
                None => DisputedTransaction {
                    kind: DisputedTransactionKind::Dispute,
                    client,
                    tx,
                }
                .into(),
            },
            TransactionType::Resolve => match amount {
                Some(_) => return Err(CsvParseError::ExpectedNoAmount),
                None => DisputedTransaction {
                    kind: DisputedTransactionKind::Resolve,
                    client,
                    tx,
                }
                .into(),
            },
            TransactionType::Chargeback => match amount {
                Some(_) => return Err(CsvParseError::ExpectedNoAmount),
                None => DisputedTransaction {
                    kind: DisputedTransactionKind::Chargeback,
                    client,
                    tx,
                }
                .into(),
            },
        };

        Ok(tx)
    }
}
