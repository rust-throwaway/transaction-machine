use serde::{Deserialize, Serialize};
use std::ops::{Add, Sub};
use thiserror::Error;

/// An account associated with a client's state.
///
/// This is its own structure to prevent direct operations on the internal values that may violate
/// any contracts.
#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Account {
    available: f64,
    held: f64,
}

impl Account {
    /// Returns the available funds in this account.
    pub fn get_available(&self) -> f64 {
        self.available
    }

    /// Returns any held funds in this account.
    pub fn get_frozen(&self) -> f64 {
        self.held
    }

    /// Returns the total value of the available and frozen funds.
    pub fn get_total(&self) -> f64 {
        self.available + self.held
    }

    /// Attempts to deposit `amount` in this account. If `amount` is negative, then an error is
    /// returned.
    pub fn deposit(&mut self, amount: f64) -> Result<(), UpdateError> {
        if amount.is_sign_negative() {
            Err(UpdateError::NegativeValue)
        } else {
            self.available = self.available.add(amount);
            Ok(())
        }
    }

    /// Attempts to withdraw `amount` in this account. If `amount` is negative, then an error is
    /// returned.
    pub fn withdraw(&mut self, amount: f64) -> Result<(), UpdateError> {
        if amount.is_sign_negative() {
            Err(UpdateError::NegativeValue)
        } else {
            let result = self.available.sub(amount);
            if result.is_sign_negative() {
                Err(UpdateError::InsufficientFunds)
            } else {
                self.available = result;
                Ok(())
            }
        }
    }

    /// Attempts to move `amount` from the available funds to the held funds. If `amount` is
    /// negative, then an error is returned.
    pub fn hold(&mut self, amount: f64) -> Result<(), UpdateError> {
        if amount.is_sign_negative() {
            Err(UpdateError::NegativeValue)
        } else {
            self.available = self.available.sub(amount);
            self.held += amount;
            Ok(())
        }
    }

    /// Releases `amount` from the available funds.
    pub fn release(&mut self, amount: f64) {
        self.available = self.available.add(amount);
        self.held -= amount;
    }

    /// Removes `amount` from the held funds.
    pub fn charge(&mut self, amount: f64) {
        self.held -= amount;
    }
}

#[derive(Debug, Error, Clone, PartialEq)]
pub enum UpdateError {
    #[error("A negative amount was provided")]
    NegativeValue,
    #[error("The account has insufficient funds")]
    InsufficientFunds,
}
