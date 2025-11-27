use blockchain_benches::bench_codecs::{AddressCodec, AmountCodec, InvalidInput, KeyCodec, TimestampCodec, TxCodec};
use blockchain_benches::bench_common::{self, run_all_parallel, run_dictionary, run_index, run_plain, run_range, Address, Amount, Key, NamedJob, Timestamp, TxHash};
use blockchain_benches::rocksdb::store::{Layout, Store, StoreError, StoreResult};
use std::path::{Path, PathBuf};

struct RocksInvalid;

impl InvalidInput<StoreError> for RocksInvalid {
	fn invalid_input(msg: &'static str) -> StoreError {
		StoreError::InvalidInput(msg.into())
	}
}

type RKeyCodec = KeyCodec<StoreError, RocksInvalid>;
type RAmountCodec = AmountCodec<StoreError, RocksInvalid>;
type RTimestampCodec = TimestampCodec<StoreError, RocksInvalid>;
type RTxCodec = TxCodec<StoreError, RocksInvalid>;
type RAddressCodec = AddressCodec<StoreError>;

fn main() -> StoreResult<()> {
	let mut args = std::env::args().skip(1);
	let mut total = 10_000_000u64;
	let mut base: Option<PathBuf> = None;
	let mut benches: Option<Vec<String>> = None;

	while let Some(arg) = args.next() {
		match arg.as_str() {
			"--total" => {
				if let Some(v) = args.next().and_then(|s| s.parse::<u64>().ok()) {
					total = v;
				}
			},
			"--dir" => {
				if let Some(p) = args.next() {
					base = Some(PathBuf::from(p));
				}
			},
			"--benches" => {
				if let Some(list) = args.next() {
					benches = Some(list.split(',').map(|s| s.to_string()).collect());
				}
			},
			_ => {},
		}
	}

	let base = base.unwrap_or_else(|| std::env::temp_dir().join(Path::new("rocksdb_bench")));

	bench_common::cleanup_dirs(&base, &["plain", "index", "range", "dictionary"]);

	let jobs: Vec<NamedJob<StoreError>> = vec![
		{
			let base = base.clone();
			NamedJob::new("plain", Box::new(move || run_plain(&base, total, rocks_plain_factory)))
		},
		{
			let base = base.clone();
			NamedJob::new("index", Box::new(move || run_index(&base, total, rocks_index_factory)))
		},
		{
			let base = base.clone();
			NamedJob::new("range", Box::new(move || run_range(&base, total, rocks_range_factory)))
		},
		{
			let base = base.clone();
			NamedJob::new("dictionary", Box::new(move || run_dictionary(&base, total, rocks_dictionary_factory)))
		},
	];

	run_all_parallel(jobs, benches.as_deref().unwrap_or(&[]))?;

	Ok(())
}

fn rocks_plain_factory(path: &Path) -> StoreResult<Store<Key, Amount, RKeyCodec, RAmountCodec>> {
	Store::open_with_options(path, Layout::plain(0), ())
}

fn rocks_index_factory(path: &Path) -> StoreResult<Store<Key, TxHash, RKeyCodec, RTxCodec>> {
	Store::open_with_options(path, Layout::unique_index(0), ())
}

fn rocks_range_factory(path: &Path) -> StoreResult<Store<Key, Timestamp, RKeyCodec, RTimestampCodec>> {
	Store::open_with_options(path, Layout::range(0), ())
}

fn rocks_dictionary_factory(path: &Path) -> StoreResult<Store<Key, Address, RKeyCodec, RAddressCodec>> {
	Store::open_with_options(path, Layout::dictionary(0), ())
}
