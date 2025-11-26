mod store;

use std::path::{Path, PathBuf};

use blockchain_benches::bench_common::{
	run_all_parallel, run_dictionary, run_index, run_plain, run_range, Address, Amount, Key16, Timestamp, TxHash,
};
use blockchain_benches::bench_codecs::{AddressCodec, AmountCodec, InvalidInput, KeyCodec, TimestampCodec, TxCodec};
use store::{Layout, Store, StoreResult};

struct ParityInvalid;

impl InvalidInput<parity_db::Error> for ParityInvalid {
	fn invalid_input(msg: &'static str) -> parity_db::Error {
		parity_db::Error::InvalidInput(msg.into())
	}
}

type PKeyCodec = KeyCodec<parity_db::Error, ParityInvalid>;
type PAmountCodec = AmountCodec<parity_db::Error, ParityInvalid>;
type PTimestampCodec = TimestampCodec<parity_db::Error, ParityInvalid>;
type PTxCodec = TxCodec<parity_db::Error, ParityInvalid>;
type PAddressCodec = AddressCodec<parity_db::Error>;

fn main() -> StoreResult<()> {
	let mut args = std::env::args().skip(1);
	let mut total = 10_000_000u64;
	let mut base: Option<PathBuf> = None;

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
			_ => {},
		}
	}

	let base = base.unwrap_or_else(|| std::env::temp_dir().join(Path::new("parity_bench")));

	blockchain_benches::bench_common::cleanup_dirs(&base, &["plain", "index", "range", "dictionary"]);

	let jobs: Vec<Box<dyn FnOnce() -> StoreResult<()> + Send>> = vec![
		{
			let base = base.clone();
			Box::new(move || run_plain(&base, total, parity_plain_factory))
		},
		{
			let base = base.clone();
			Box::new(move || run_index(&base, total, parity_index_factory))
		},
		{
			let base = base.clone();
			Box::new(move || run_range(&base, total, parity_range_factory))
		},
		{
			let base = base.clone();
			Box::new(move || run_dictionary(&base, total, parity_dictionary_factory))
		},
	];

	run_all_parallel(jobs)?;

	Ok(())
}

fn parity_plain_factory(path: &Path) -> StoreResult<Store<Key16, Amount, PKeyCodec, PAmountCodec>> {
	Store::open_with_options(path, Layout::plain(0), ())
}

fn parity_index_factory(path: &Path) -> StoreResult<Store<Key16, TxHash, PKeyCodec, PTxCodec>> {
	Store::open_with_options(path, Layout::unique_index(0), ())
}

fn parity_range_factory(path: &Path) -> StoreResult<Store<Key16, Timestamp, PKeyCodec, PTimestampCodec>> {
	Store::open_with_options(path, Layout::range(0), ())
}

fn parity_dictionary_factory(path: &Path) -> StoreResult<Store<Key16, Address, PKeyCodec, PAddressCodec>> {
	Store::open_with_options(path, Layout::dictionary(0), ())
}
