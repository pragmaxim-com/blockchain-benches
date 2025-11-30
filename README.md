Given that : 
- Relying on the benchmarks of the authors is not a good idea. 
- Benchmark should be always done in the context of the target application.

In the context of persisting :
- billions of hash keys or linearly increasing unique numeric keys
- in parallel

We suffer from :
- Dual Ordering Problem : supporting queries efficiently on both Key and Value requires two different index orders, and maintaining them consistently creates problems of duplication.
- Increasing write amplification with # of rows 

so I compare these storages with basic abstraction like index, range and dictionary of many columns because that reveals real write amplification :
- [fjall](https://github.com/fjall-rs/fjall) store
  - LSM-tree-based storage (tuned here for higher write throughput; weaker reads)
- [parity](https://github.com/paritytech/parity-db/) store 
  - mmapped dynamically sized probing hash tables with only 2 BTrees 
- [fst-lsm](https://github.com/BurntSushi/fst) store
  - similar to LSM Tree with mempool and no WAL but with fst instead of sstables
  - FST (Finite State Transducer) can hold `u64` value and can be :
      - merged and perform arbitrary operation on the values, like sum
      - merged in parallel so that available parallelism for node levels can be split, for instance total par of 16 :
          - block : 1
          - txs : 5
          - utxos : 10
- [redb](https://github.com/cberner/redb) store
  - copy-on-write B+Tree style embedded database
- [rocksdb](https://github.com/facebook/rocksdb/) store
  - classic LSM with WAL and column families
- [libmdbx](https://github.com/erthink/libmdbx) store
  - append-friendly B+Tree with configurable sync levels

Bench CLI helpers (each accepts `--benches <comma list>` with `plain,index,range,dictionary,all_in_par`):
- From the workspace root, target the specific package/bin (workspace split avoids compiling all backends):
  - `cargo run -p parity-bench --release --bin parity -- [--total <rows>] [--dir <path>] [--benches <list>]`
  - `cargo run -p fjall-bench --release --bin fjall -- [--total <rows>] [--dir <path>] [--benches <list>]`
  - `cargo run -p fst-bench --release --bin fst -- [--total <rows>] [--mem-mb <megabytes>] [--dir <path>] [--benches <list>]`
  - `cargo run -p redb-bench --release --bin redb -- [--total <rows>] [--dir <path>] [--benches <list>]`
  - `cargo run -p rocksdb-bench --release --bin rocksdb -- [--total <rows>] [--dir <path>] [--benches <list>]`
  - `cargo run -p mdbx-bench --release --bin mdbx -- [--total <rows>] [--dir <path>] [--benches <list>]`
  - FST txhash-only build from an existing Fjall index: `cargo run -p fst --release --bin fst-txhash-bench -- [--source <fjall_dir>] [--dir <path>]`

Defaults: 10_000_000 rows, temp dir; all benches in parallel

### Results

**LSM Trees**
RocksDB is unbeatable since release `10.7.3` as it improved performance for my use case ~ 50%
due to various optimizations like parallel compression.

RocksDB keeps indexing `throughput ~640K rows/sec` (on common hardware) into 2 columns KV/VK (32bytes hash and u32) at `2B rows at each`, occupying 175GB.
And beats all others in speed, space or write amplification, it just consumes much more CPU than BTrees and has worse query performance.
BTrees are simply IO bound by too much random access.

**BTrees**
`libmdbx` wins however it is at 40% of RocksDB `10.7.3` indexing throughput, with 250GB used, Btrees need more disk space and less CPU.

### Conclusion

`RocksDB` wins in write-only workload.
`libmdbx` wins in combined workload because BTrees shine in there.

For blockchains, if you resolve prev-outs during indexing, `libmdbx` might outperform RocksDB.
The bench should be actually performed on real BTC data because BTree and LSMTree perform differently on uniform vs Zipfian distributions.