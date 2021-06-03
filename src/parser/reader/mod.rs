#[cfg(test)]
mod tests;

use crate::parser::CsvTransaction;
use crate::transaction::Transaction;
use csv::{ReaderBuilder, Trim};
use std::convert::TryFrom;
use std::error::Error;
use std::path::PathBuf;
use thiserror::Error;
use tokio::sync::mpsc;

const FORWARD_CHANNEL_CLOSED: &str = "Transaction IO closed unexpectedly";

#[derive(Error, Debug)]
pub enum ReaderError {
    #[error("An IO error was produced: `{0}`")]
    Io(String),
    #[error("An error was produced when parsing a record: `{0}`")]
    Parse(String),
    #[error("An error was produced when handling a CSV record: `{0}`")]
    Csv(Box<dyn Error + Send>),
}

/// Creates a task which will read the CSV file `path`, deserialize the records and send them over
/// the `sender` channel.
pub async fn reader_task(
    path: PathBuf,
    sender: mpsc::Sender<Transaction>,
) -> Result<(), ReaderError> {
    // Reader performs internal buffering so there's no need to use a BufReader
    let reader = ReaderBuilder::new()
        .trim(Trim::All)
        .flexible(true)
        .has_headers(true)
        .from_path(path)
        .map_err(|e| ReaderError::Csv(Box::new(e)))?
        .into_deserialize::<CsvTransaction>();

    for parse_result in reader {
        match parse_result {
            Ok(csv_tx) => {
                let tx =
                    Transaction::try_from(csv_tx).map_err(|e| ReaderError::Parse(e.to_string()))?;
                if sender.send(tx).await.is_err() {
                    return Err(ReaderError::Io(FORWARD_CHANNEL_CLOSED.to_string()));
                }
            }
            Err(e) => return Err(ReaderError::Csv(Box::new(e))),
        }
    }

    Ok(())
}
