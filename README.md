Given that : 
 - Relying on the benchmarks of the authors is not a good idea. 
 - Benchmark should be always done in the context of the target application.

In the context of persisting :
 - billions of hash keys or linearly increasing unique numeric keys
 - in parallel

I am adding benchmarks of :
 - copy-on-write (COW) B+Tree family  (redb, sled, libmdbx)
 - LSM Tree with WAL family (leveldb, rocksdb, tikv)
 - mmap-backed dynamically sized probing hash tables (paritydb)
