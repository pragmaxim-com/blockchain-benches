Getting the most out of :
 - copy-on-write (COW) B+Tree family  (redb, sled, libmdbx)
 - LSM Tree with WAL family (leveldb, rocksdb, tikv)
 - mmap-backed dynamically sized probing hash tables (paritydb)

Given that we need to persist billions of KVs with high r/w throughput