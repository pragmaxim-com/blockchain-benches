mod store;

use std::path::{Path, PathBuf};

use core::{
	bench_codecs::{AddressCodec, AmountCodec, InvalidInput, KeyCodec, TimestampCodec, TxCodec},
	bench_common::{run_all_parallel, run_dictionary, run_index, run_plain, run_range, Address, Amount, Key, NamedJob, Timestamp, TxHash},
};
use store::{Layout, Store, StoreResult};
use parity_db::Error as PError;

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

	let base = base.unwrap_or_else(|| std::env::temp_dir().join(Path::new("parity_bench")));

	core::bench_common::cleanup_dirs(&base, &["plain", "index", "range", "dictionary"]);

	let jobs: Vec<NamedJob<PError>> = vec![
		{
			let base = base.clone();
			NamedJob::new("plain", Box::new(move || run_plain(&base, total, parity_plain_factory)))
		},
		{
			let base = base.clone();
			NamedJob::new("index", Box::new(move || run_index(&base, total, parity_index_factory)))
		},
		{
			let base = base.clone();
			NamedJob::new("range", Box::new(move || run_range(&base, total, parity_range_factory)))
		},
		{
			let base = base.clone();
			NamedJob::new("dictionary", Box::new(move || run_dictionary(&base, total, parity_dictionary_factory)))
		},
	];

	run_all_parallel(jobs, benches.as_deref().unwrap_or(&[]))?;

	Ok(())
}

fn parity_plain_factory(path: &Path) -> StoreResult<Store<Key, Amount, PKeyCodec, PAmountCodec>> {
	Store::open_with_options(path, Layout::plain(0), ())
}

fn parity_index_factory(path: &Path) -> StoreResult<Store<Key, TxHash, PKeyCodec, PTxCodec>> {
	Store::open_with_options(path, Layout::unique_index(0), ())
}

fn parity_range_factory(path: &Path) -> StoreResult<Store<Key, Timestamp, PKeyCodec, PTimestampCodec>> {
	Store::open_with_options(path, Layout::range(0), ())
}

fn parity_dictionary_factory(path: &Path) -> StoreResult<Store<Key, Address, PKeyCodec, PAddressCodec>> {
	Store::open_with_options(path, Layout::dictionary(0), ())
}
