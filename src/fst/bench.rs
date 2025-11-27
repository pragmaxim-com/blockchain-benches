use std::path::{Path, PathBuf};

use blockchain_benches::bench_codecs::{AddressCodec, AmountCodec, InvalidInput, KeyCodec, TimestampCodec, TxCodec};
use blockchain_benches::bench_common::{
    run_all_parallel, run_dictionary, run_index, run_plain, run_range, Address, Amount, Key, NamedJob, Timestamp, TxHash,
};
use blockchain_benches::fst::store::{Layout, Store, StoreOptions, StoreResult};
use blockchain_benches::fst::store;

const AVG_ADDRESS_BYTES: usize = 64;

struct FstInvalid;

impl InvalidInput<store::StoreError> for FstInvalid {
	fn invalid_input(msg: &'static str) -> store::StoreError {
		store::StoreError::InvalidInput(msg.into())
	}
}

type FKeyCodec = KeyCodec<store::StoreError, FstInvalid>;
type FAmountCodec = AmountCodec<store::StoreError, FstInvalid>;
type FTimestampCodec = TimestampCodec<store::StoreError, FstInvalid>;
type FTxCodec = TxCodec<store::StoreError, FstInvalid>;
type FAddressCodec = AddressCodec<store::StoreError>;

fn main() -> StoreResult<()> {
    let mut args = std::env::args().skip(1);
    let mut total = 10_000_000u64;
    let mut mem_budget_bytes = store::DEFAULT_MEMTABLE_BUDGET_BYTES;
    let mut base: Option<PathBuf> = None;
    let mut benches: Option<Vec<String>> = None;

	while let Some(arg) = args.next() {
		match arg.as_str() {
			"--total" => {
				if let Some(v) = args.next().and_then(|s| s.parse::<u64>().ok()) {
					total = v;
				}
			},
			"--mem-mb" => {
				if let Some(v) = args.next().and_then(|s| s.parse::<u64>().ok()) {
					mem_budget_bytes = (v as usize).saturating_mul(1024 * 1024).max(1);
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

	let base = base.unwrap_or_else(|| std::env::temp_dir().join(Path::new("fst_bench")));

	let plain_opts = StoreOptions::from_estimates(total, 16 + 8, mem_budget_bytes);
	let index_opts = StoreOptions::from_estimates(total, 16 + 32, mem_budget_bytes);
	let range_opts = StoreOptions::from_estimates(total, 16 + 8, mem_budget_bytes);
	let dict_opts = StoreOptions::from_estimates(total, 16 + AVG_ADDRESS_BYTES, mem_budget_bytes);

	blockchain_benches::bench_common::cleanup_dirs(&base, &["merge", "plain", "index", "range", "dictionary"]);

    let jobs: Vec<NamedJob<store::StoreError>> = vec![
        {
            let base = base.clone();
            NamedJob::new("plain", Box::new(move || run_plain(&base, total, move |path| fst_plain_factory(path, plain_opts))))
        },
        {
            let base = base.clone();
            NamedJob::new("index", Box::new(move || run_index(&base, total, move |path| fst_index_factory(path, index_opts))))
        },
        {
            let base = base.clone();
            NamedJob::new("range", Box::new(move || run_range(&base, total, move |path| fst_range_factory(path, range_opts))))
        },
        {
            let base = base.clone();
            NamedJob::new("dictionary", Box::new(move || run_dictionary(&base, total, move |path| fst_dictionary_factory(path, dict_opts))))
        },
    ];

    run_all_parallel(jobs, benches.as_deref().unwrap_or(&[]))?;

	// Final compaction into a single segment per column to ease reads.
	let mut plain_store = fst_plain_factory(&base.join("plain"), plain_opts)?;
	plain_store.multi_way_merge()?;
	let mut index_store = fst_index_factory(&base.join("index"), index_opts)?;
	index_store.multi_way_merge()?;
	let mut range_store = fst_range_factory(&base.join("range"), range_opts)?;
	range_store.multi_way_merge()?;
	let mut dict_store = fst_dictionary_factory(&base.join("dictionary"), dict_opts)?;
	dict_store.multi_way_merge()?;

	Ok(())
}

fn fst_plain_factory(path: &Path, options: StoreOptions) -> StoreResult<Store<Key, Amount, FKeyCodec, FAmountCodec>> {
	Store::open(path, Layout::plain(0), options)
}

fn fst_index_factory(path: &Path, options: StoreOptions) -> StoreResult<Store<Key, TxHash, FKeyCodec, FTxCodec>> {
	Store::open(path, Layout::unique_index(0), options)
}

fn fst_range_factory(path: &Path, options: StoreOptions) -> StoreResult<Store<Key, Timestamp, FKeyCodec, FTimestampCodec>> {
	Store::open(path, Layout::range(0), options)
}

fn fst_dictionary_factory(path: &Path, options: StoreOptions) -> StoreResult<Store<Key, Address, FKeyCodec, FAddressCodec>> {
	Store::open(path, Layout::dictionary(0), options)
}
