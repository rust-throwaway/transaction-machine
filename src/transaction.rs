use serde::{Deserialize, Serialize};

/// An abstraction over transfer and disputed transactions.
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum Transaction {
    /// The transaction is either a `deposit` or a `withdrawal`.
    Transfer(TransferTransaction),
    /// The transaction is either a `dispute`, `resolve` or `chargeback`.
    Disputed(DisputedTransaction),
}

impl From<TransferTransaction> for Transaction {
    fn from(tx: TransferTransaction) -> Self {
        Transaction::Transfer(tx)
    }
}

impl From<DisputedTransaction> for Transaction {
    fn from(tx: DisputedTransaction) -> Self {
        Transaction::Disputed(tx)
    }
}

impl Transaction {
    /// Returns the client ID associated with this transaction.
    pub fn client_id(&self) -> u16 {
        match self {
            Transaction::Transfer(tx) => tx.client,
            Transaction::Disputed(tx) => tx.client,
        }
    }

    /// Returns the ID associated with this transaction.
    pub fn id(&self) -> u32 {
        match self {
            Transaction::Transfer(tx) => tx.tx,
            Transaction::Disputed(tx) => tx.tx,
        }
    }

    /// Creates a new deposit transaction.
    pub fn deposit(client: u16, tx: u32, amount: f64) -> Transaction {
        Transaction::Transfer(TransferTransaction {
            kind: TransferTransactionKind::Deposit,
            client,
            tx,
            amount,
            disputed: DisputeStatus::NotDisputed,
        })
    }

    /// Creates a new withdrawal transaction.
    pub fn withdrawal(client: u16, tx: u32, amount: f64) -> Transaction {
        Transaction::Transfer(TransferTransaction {
            kind: TransferTransactionKind::Withdrawal,
            client,
            tx,
            amount,
            disputed: DisputeStatus::NotDisputed,
        })
    }

    /// Creates a new dispute transaction.
    pub fn dispute(client: u16, tx: u32) -> Transaction {
        Transaction::Disputed(DisputedTransaction {
            kind: DisputedTransactionKind::Dispute,
            client,
            tx,
        })
    }

    /// Creates a new resolve transaction.
    pub fn resolve(client: u16, tx: u32) -> Transaction {
        Transaction::Disputed(DisputedTransaction {
            kind: DisputedTransactionKind::Resolve,
            client,
            tx,
        })
    }

    /// Creates a new chargeback transaction.
    pub fn chargeback(client: u16, tx: u32) -> Transaction {
        Transaction::Disputed(DisputedTransaction {
            kind: DisputedTransactionKind::Chargeback,
            client,
            tx,
        })
    }
}

/// A transfer transaction model.
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct TransferTransaction {
    /// The type of transfer transaction.
    #[serde(rename = "type")]
    pub kind: TransferTransactionKind,
    /// The associated client ID.
    pub client: u16,
    /// A unique transaction number.
    pub tx: u32,
    /// The value of the transaction.
    pub amount: f64,
    /// Whether this transaction is marked as disputed.
    pub disputed: DisputeStatus,
}

/// A flag associated with a transaction's dispute status.
#[derive(Debug, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub enum DisputeStatus {
    /// The transaction is not and has never been disputed.
    NotDisputed,
    /// The transaction is currently under dispute.
    Disputed,
    /// The transaction was previously disputed but it has been resolved.
    Resolved,
}

impl Default for DisputeStatus {
    fn default() -> Self {
        DisputeStatus::NotDisputed
    }
}

impl TransferTransaction {
    #[cfg(test)]
    pub fn is_disputed(&self) -> bool {
        matches!(self.disputed, DisputeStatus::Disputed)
    }

    #[cfg(test)]
    pub fn is_resolved(&self) -> bool {
        matches!(self.disputed, DisputeStatus::Resolved)
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransferTransactionKind {
    Deposit,
    Withdrawal,
}

/// A disputed transaction model.
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct DisputedTransaction {
    /// The type of disputed transaction.
    #[serde(rename = "type")]
    pub kind: DisputedTransactionKind,
    /// The associated client ID.
    pub client: u16,
    /// A unique transaction number.
    pub tx: u32,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DisputedTransactionKind {
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

impl From<TransferTransactionKind> for TransactionType {
    fn from(kind: TransferTransactionKind) -> Self {
        match kind {
            TransferTransactionKind::Deposit => TransactionType::Deposit,
            TransferTransactionKind::Withdrawal => TransactionType::Withdrawal,
        }
    }
}

impl From<DisputedTransactionKind> for TransactionType {
    fn from(kind: DisputedTransactionKind) -> Self {
        match kind {
            DisputedTransactionKind::Resolve => TransactionType::Resolve,
            DisputedTransactionKind::Dispute => TransactionType::Dispute,
            DisputedTransactionKind::Chargeback => TransactionType::Chargeback,
        }
    }
}
