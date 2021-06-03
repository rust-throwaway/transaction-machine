#![deny(
    missing_copy_implementations,
    missing_debug_implementations,
    trivial_casts,
    trivial_numeric_casts,
    unstable_features,
    unused_must_use,
    unused_mut,
    unused_imports,
    unused_import_braces
)]

mod client;
mod data;
mod db;
mod io;
mod parser;
mod transaction;

use crate::client::ClientStore;
use crate::db::{DiskStore, StoreError};
use crate::io::IoTask;

use crate::data::generate_csv;
use crate::parser::reader::{reader_task, ReaderError};
use crate::parser::writer::write_state;
use futures::future::try_join;
use futures::TryFutureExt;
use std::env;
use std::path::Path;
use std::str::FromStr;
use thiserror::Error;
use tokio::sync::mpsc;

const IO_BUFFER_SIZE: usize = 256;
const BRIDGE_BUFFER_SIZE: usize = 1024;
const GENERATE_COMMAND: &str = "generate";
const DEFAULT_DIR: &str = "store";

#[tokio::main]
async fn main() -> Result<(), TaskError> {
    let mut args = env::args().into_iter().skip(1);
    let command = args.next();

    match command.as_deref() {
        Some(GENERATE_COMMAND) => {
            let count = args.next().expect("Generator count not specified");
            match usize::from_str(count.as_str()) {
                Ok(count) => {
                    generate_csv(count);
                    Ok(())
                }
                Err(e) => {
                    panic!("Failed to parse count: `{:?}`", e)
                }
            }
        }
        Some(file) => run(file).await,
        None => panic!("Missing argument"),
    }
}

/// Asynchronously runs the payments machine. Serving `input_file`.
async fn run<P: AsRef<Path>>(input_file: P) -> Result<(), TaskError> {
    let (tx, rx) = mpsc::channel(IO_BUFFER_SIZE);

    let store = ClientStore::new(DiskStore::new(DEFAULT_DIR)?);
    let io_task = IoTask::new(rx, store.clone())
        .run(BRIDGE_BUFFER_SIZE)
        .map_err(TaskError::Store);
    let reader_task = reader_task(input_file.as_ref().to_path_buf(), tx).map_err(TaskError::Reader);

    let io_result = try_join(io_task, reader_task).await;
    match io_result {
        Ok((_, _)) => {
            write_state(store)?;
        }
        Err(e) => {
            panic!("Processor failed with `{:?}`", e)
        }
    }
    Ok(())
}

#[derive(Error, Debug)]
enum TaskError {
    #[error("An error was produced by the reader task: `{0}`")]
    Reader(ReaderError),
    #[error("An error was produced by the store: `{0}`")]
    Store(StoreError),
}

impl From<ReaderError> for TaskError {
    fn from(e: ReaderError) -> Self {
        TaskError::Reader(e)
    }
}

impl From<StoreError> for TaskError {
    fn from(e: StoreError) -> Self {
        TaskError::Store(e)
    }
}
