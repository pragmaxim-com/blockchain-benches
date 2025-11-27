Given that : 
 - Relying on the benchmarks of the authors is not a good idea. 
 - Benchmark should be always done in the context of the target application.

In the context of persisting :
 - billions of hash keys or linearly increasing unique numeric keys
 - in parallel

We suffer from **Dual Ordering Problem** : 
 - supporting queries efficiently on both Key and Value requires two different index orders, and maintaining them consistently creates problems of duplication.

so I compare these storages with basic abstraction like index, range and dictionary of many columns because that reveals real write amplification :
- [fjall](https://github.com/fjall-rs/fjall) store
  - LSM-tree-based storage (tuned here for higher write throughput; weaker reads)
- [parity](https://github.com/paritytech/parity-db/) store 
  - mmapped dynamically sized probing hash tables with only 2 BTrees 
- [fst-lsm](https://github.com/BurntSushi/fst) store
  - similar to LSM Tree with mempool and no WAL but with fst instead of sstables
- [redb](https://github.com/cberner/redb) store
  - copy-on-write B+Tree style embedded database
- [rocksdb](https://github.com/facebook/rocksdb/) store
  - classic LSM with WAL and column families
- [libmdbx](https://github.com/erthink/libmdbx) store
  - append-friendly B+Tree (LMDB successor) with configurable sync levels

Bench CLI helpers (each accepts `--benches <comma list>` with `plain,index,range,dictionary,all`):
- From the workspace root, target the specific package/bin (workspace split avoids compiling all backends):
  - `cargo run -p parity --release --bin parity -- [--total <rows>] [--dir <path>] [--benches <list>]`
  - `cargo run -p fjall --release --bin fjall -- [--total <rows>] [--dir <path>] [--benches <list>]`
  - `cargo run -p fst --release --bin fst -- [--total <rows>] [--mem-mb <megabytes>] [--dir <path>] [--benches <list>]`
  - `cargo run -p redb --release --bin redb -- [--total <rows>] [--dir <path>] [--benches <list>]`
  - `cargo run -p rocksdb --release --bin rocksdb -- [--total <rows>] [--dir <path>] [--benches <list>]`
  - `cargo run -p mdbx --release --bin mdbx -- [--total <rows>] [--dir <path>] [--benches <list>]`
  - FST txhash-only build from an existing Fjall index: `cargo run -p fst --release --bin fst-txhash-bench -- [--source <fjall_dir>] [--dir <path>]`

Defaults: 10_000_000 rows, temp dir; fst uses a 2 GB memtable; `--benches` empty or `all_in_par` runs all in parallel.

### Results

RocksDB is unbeatable since release `10.7.3` as it improved performance ~ 50% due to various optimizations parallel compression, etc.