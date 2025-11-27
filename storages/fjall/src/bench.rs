mod store;

use std::path::{Path, PathBuf};

use core::bench_codecs::{AddressCodec, AmountCodec, InvalidInput, KeyCodec, TimestampCodec, TxCodec};
use core::bench_common::{
	run_all_parallel, run_dictionary, run_index, run_plain, run_range, Address, Amount, Key, NamedJob, Timestamp, TxHash,
};
use crate::store::{FjallOptions, Layout, Store, StoreError, StoreResult};

struct FjallInvalid;

impl InvalidInput<StoreError> for FjallInvalid {
	fn invalid_input(msg: &'static str) -> StoreError {
		StoreError::InvalidInput(msg.into())
	}
}

type FKeyCodec = KeyCodec<StoreError, FjallInvalid>;
type FAmountCodec = AmountCodec<StoreError, FjallInvalid>;
type FTimestampCodec = TimestampCodec<StoreError, FjallInvalid>;
type FTxCodec = TxCodec<StoreError, FjallInvalid>;
type FAddressCodec = AddressCodec<StoreError>;
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

	let base = base.unwrap_or_else(|| std::env::temp_dir().join(Path::new("fjall_bench")));

	core::bench_common::cleanup_dirs(&base, &["plain", "index", "range", "dictionary"]);

    let jobs: Vec<NamedJob<StoreError>> = vec![
        {
            let base = base.clone();
            NamedJob::new("plain", Box::new(move || run_plain(&base, total, fjall_plain_factory)))
        },
        {
            let base = base.clone();
            NamedJob::new("index", Box::new(move || run_index(&base, total, fjall_index_factory)))
        },
        {
            let base = base.clone();
            NamedJob::new("range", Box::new(move || run_range(&base, total, fjall_range_factory)))
        },
        {
            let base = base.clone();
            NamedJob::new("dictionary", Box::new(move || run_dictionary(&base, total, fjall_dictionary_factory)))
        },
    ];

    run_all_parallel(jobs, benches.as_deref().unwrap_or(&[]))?;

	Ok(())
}

fn fjall_plain_factory(path: &Path) -> StoreResult<Store<Key, Amount, FKeyCodec, FAmountCodec>> {
	Store::open_with_options(path, Layout::plain(0), FjallOptions::default())
}

fn fjall_index_factory(path: &Path) -> StoreResult<Store<Key, TxHash, FKeyCodec, FTxCodec>> {
	Store::open_with_options(path, Layout::unique_index(0), FjallOptions::default())
}

fn fjall_range_factory(path: &Path) -> StoreResult<Store<Key, Timestamp, FKeyCodec, FTimestampCodec>> {
	Store::open_with_options(path, Layout::range(0), FjallOptions::default())
}

fn fjall_dictionary_factory(path: &Path) -> StoreResult<Store<Key, Address, FKeyCodec, FAddressCodec>> {
	Store::open_with_options(path, Layout::dictionary(0), FjallOptions::default())
}
