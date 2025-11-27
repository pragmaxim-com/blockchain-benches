use std::path::{Path, PathBuf};

use blockchain_benches::bench_codecs::{AddressCodec, AmountCodec, InvalidInput, KeyCodec, TimestampCodec, TxCodec};
use blockchain_benches::bench_common::{self, run_all_parallel, run_dictionary, run_index, run_plain, run_range, Address, Amount, Key, NamedJob, Timestamp, TxHash};
use blockchain_benches::redb::store::{Layout, Store, StoreError, StoreResult};

struct RedbInvalid;

impl InvalidInput<StoreError> for RedbInvalid {
	fn invalid_input(msg: &'static str) -> StoreError {
		StoreError::InvalidInput(msg.into())
	}
}

type RKeyCodec = KeyCodec<StoreError, RedbInvalid>;
type RAmountCodec = AmountCodec<StoreError, RedbInvalid>;
type RTimestampCodec = TimestampCodec<StoreError, RedbInvalid>;
type RTxCodec = TxCodec<StoreError, RedbInvalid>;
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

	let base = base.unwrap_or_else(|| std::env::temp_dir().join(Path::new("redb_bench")));

    bench_common::cleanup_dirs(&base, &["plain", "index", "range", "dictionary"]);

    let jobs: Vec<NamedJob<StoreError>> = vec![
        {
            let base = base.clone();
            NamedJob::new("plain", Box::new(move || run_plain(&base, total, redb_plain_factory)))
        },
        {
            let base = base.clone();
            NamedJob::new("index", Box::new(move || run_index(&base, total, redb_index_factory)))
        },
        {
            let base = base.clone();
            NamedJob::new("range", Box::new(move || run_range(&base, total, redb_range_factory)))
        },
        {
            let base = base.clone();
            NamedJob::new("dictionary", Box::new(move || run_dictionary(&base, total, redb_dictionary_factory)))
        },
    ];

    run_all_parallel(jobs, benches.as_deref().unwrap_or(&[]))?;

	Ok(())
}

fn redb_plain_factory(path: &Path) -> StoreResult<Store<Key, Amount, RKeyCodec, RAmountCodec>> {
	Store::open_with_options(path, Layout::plain(), ())
}

fn redb_index_factory(path: &Path) -> StoreResult<Store<Key, TxHash, RKeyCodec, RTxCodec>> {
	Store::open_with_options(path, Layout::unique_index(), ())
}

fn redb_range_factory(path: &Path) -> StoreResult<Store<Key, Timestamp, RKeyCodec, RTimestampCodec>> {
	Store::open_with_options(path, Layout::range(), ())
}

fn redb_dictionary_factory(path: &Path) -> StoreResult<Store<Key, Address, RKeyCodec, RAddressCodec>> {
	Store::open_with_options(path, Layout::dictionary(), ())
}
