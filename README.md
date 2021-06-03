# Rust toy payments engine
This repository contains a simple, toy payments engine implemented using Rust.

## Usage

---
To generate data for the application. Run
```
cargo run generate count
```

Where `count` is the number of transactions to be generated. This will output a file named `generated.csv`.

---

To run the application:
```
cargo run input.csv
```

Where `input.csv` is the name of the CSV file to process.

# Assumptions
- Only a deposit can be disputed.
- It is possible for a client's balance to drop below zero if a deposit that was made has been withdrawn and then a dispute is made on the original transaction.
- Negative transaction amounts cannot be processed.

# Decisions
- As transactions could be disputed for a long period of time (weeks), they're not held directly with the `client` structure and are persisted.
- Individual components should be thoroughly tested. A data generator is present in the application that will generate a number of clients and transactions that can be used to test the application for robustness
- A store is used so that if the application crashes, or the host is stopped, no data is lost and it can be recovered from the store. When a client ID is read, it is first checked to see if an associated state has been persisted. If one exists, then this is used to rebuild the client.
- The architecture of this application is designed such that the components can be composed easily. As such, the IO task is agnostic of its source and this would allow for the input stream to be something other than a CSV reader: such as a TCP stream. This approach also makes it easier to switch away from RocksDB to another store.
- Tracing support is implemented to aid in viewing the execution state of the application. A new span is entered when a client is started and is scoped by its ID.
- Any dependencies used should have a flexible enough licence for use in commercial applications.

# Possible improvements
- Float printing could be sped up using [ryu](https://github.com/dtolnay/ryu).
- The IO task could be improved through the addition of mailboxes for each client and deferring the transaction tasks in to a `FuturesOrdered`. This would maintain the consistency through the transactions being executed but possibly improve the throughput of the application.
- The error handling of the application could be more graceful and not build up error chains. At present, the application will fail only when a store error is produced - as this would result in data inconsistencies if the application continued.
- The testing strategy of this application could be changed to include fuzz testing through using [cargo-fuzz](https://rust-fuzz.github.io/book/cargo-fuzz.html) or [mutagen](https://github.com/llogiq/mutagen).
- Improve the CLI usage. The CLI argument handling at present is a bit crude and could be improved through using [CLAP](https://github.com/clap-rs/clap). With the addition of a configuration file for certain parameters (such as buffer sizes) the application would be friendlier to use.
- A metrics system could be implemented in to the application which would track the number of transactions that are being processed and results could be written to another file.
