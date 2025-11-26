Given that : 
 - Relying on the benchmarks of the authors is not a good idea. 
 - Benchmark should be always done in the context of the target application.

In the context of persisting :
 - billions of hash keys or linearly increasing unique numeric keys
 - in parallel

We suffer from **Dual Ordering Problem** : 
 - supporting queries efficiently on both Key and Value requires two different index orders, and maintaining them consistently creates problems of duplication.

I already know that parity-db or fjall-rs can beat well known storages for blockchain data persistence :
 - copy-on-write (COW) B+Tree family  (redb, sled, libmdbx)
 - LSM Tree with WAL family (leveldb, rocksdb)

so I compare these storages with basic abstraction like index, range and dictionary of many columns because that reveals real write amplification :
- [fjall](https://github.com/paritytech/parity-db/) store
  - LSM-tree-based storage similar to RocksDB
- [parity](https://github.com/paritytech/parity-db/) store 
  - mmapped dynamically sized probing hash tables with only 2 BTrees 
- [fst-lsm](https://github.com/BurntSushi/fst) store
   - similar to LSM Tree with mempool and no WAL but with fst instead of sstables

Bench CLI helpers:
- `cargo run --release --bin parity-bench -- [--total <rows>] [--dir <path>]`
- `cargo run --release --bin fst-bench -- [--total <rows>] [--mem-mb <megabytes>] [--dir <path>]`

Defaults: 10_000_000 rows, temp dir; fst uses a 2 GB memtable

### Goal

The goal is to make a hybrid storage engine that combines fjall-rs with fst-lsm such that :
 - fjall is used for key-value pairs
 - fst-lsm is used for value-key pairs (index) 
