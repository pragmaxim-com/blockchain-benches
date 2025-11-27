Given that : 
 - Relying on the benchmarks of the authors is not a good idea. 
 - Benchmark should be always done in the context of the target application.

In the context of persisting :
 - billions of hash keys or linearly increasing unique numeric keys
 - in parallel

We suffer from **Dual Ordering Problem** : 
 - supporting queries efficiently on both Key and Value requires two different index orders, and maintaining them consistently creates problems of duplication.

I already know that parity-db or fjall-rs can beat well known storages for blockchain data persistence :
 - copy-on-write (COW) B+Tree family  (sled, libmdbx)
 - LSM Tree with WAL family (leveldb, rocksdb)

so I compare these storages with basic abstraction like index, range and dictionary of many columns because that reveals real write amplification :
- [fjall](https://github.com/fjall-rs/fjall) store
  - LSM-tree-based storage (tuned here for higher write throughput; weaker reads)
- [parity](https://github.com/paritytech/parity-db/) store 
  - mmapped dynamically sized probing hash tables with only 2 BTrees 
- [fst-lsm](https://github.com/BurntSushi/fst) store
  - similar to LSM Tree with mempool and no WAL but with fst instead of sstables
- [redb](https://github.com/cberner/redb) store
  - copy-on-write B+Tree style embedded database
- rocksdb store
  - classic LSM with WAL and column families

Bench CLI helpers (each accepts `--benches <comma list>` with `plain,index,range,dictionary,all`):
- `cargo run --release --bin parity-bench -- [--total <rows>] [--dir <path>] [--benches <list>]`
- `cargo run --release --bin fjall-bench -- [--total <rows>] [--dir <path>] [--benches <list>]`
- `cargo run --release --bin fst-bench -- [--total <rows>] [--mem-mb <megabytes>] [--dir <path>] [--benches <list>]`
- `cargo run --release --bin redb-bench -- [--total <rows>] [--dir <path>] [--benches <list>]`
- `cargo run --release --bin rocksdb-bench -- [--total <rows>] [--dir <path>] [--benches <list>]`
- FST txhash-only build from an existing Fjall index: `cargo run --release --bin fst-txhash-bench -- [--source <fjall_dir>] [--dir <path>]`

Defaults: 10_000_000 rows, temp dir; fst uses a 2 GB memtable; `--benches` empty or `all_in_par` runs all in parallel.

### Goal

The goal is to make a hybrid storage engine that combines fjall-rs with fst-lsm such that :
 - fjall is used for key-value pairs
 - fst-lsm is used for value-key pairs (index) 

The reason is that LSM Tree performance degrades for hashes as keys basically linearly and we will try to prove
that fst-lsm performance degrades sub-linearly. 
