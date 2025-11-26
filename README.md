Given that : 
 - Relying on the benchmarks of the authors is not a good idea. 
 - Benchmark should be always done in the context of the target application.

In the context of persisting :
 - billions of hash keys or linearly increasing unique numeric keys
 - in parallel

We suffer from **Dual Ordering Problem** : 
 - supporting queries efficiently on both Key and Value requires two different index orders, and maintaining them consistently creates problems of duplication.

I already know that for blockchain data persistence, [parity-db](https://github.com/paritytech/parity-db/) with its mmapped dynamically sized probing hash tables can beat well known storages :
 - copy-on-write (COW) B+Tree family  (redb, sled, libmdbx)
 - LSM Tree with WAL family (leveldb, rocksdb)
so I compare these storages with basic abstraction like index, range or dictionary :
- [parity](https://github.com/paritytech/parity-db/) store 
  - mmapped dynamically sized probing hash tables with only 2 BTrees 
- [fst-lsm](https://github.com/BurntSushi/fst) store
   - similar to LSM Tree with mempool and no WAL but with fst instead of sstables

Bench CLI helpers:
- `cargo run --release --bin parity-bench -- [--total <rows>] [--dir <path>]`
- `cargo run --release --bin fst-bench -- [--total <rows>] [--mem-mb <megabytes>] [--dir <path>]`

Defaults: 10_000_000 rows, temp dir; fst uses a 2 GB memtable

### Results

The bench shows that `fst-lsm` beats parity-db which is already blazing fast 
