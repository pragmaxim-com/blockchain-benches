use std::{
	fs::File,
	io::BufWriter,
	path::{Path, PathBuf},
};
use fjall::{Config, PartitionCreateOptions};
use core::store_interface::ProgressTracker;
use fst::MapBuilder;

type BenchResult<T> = Result<T, Box<dyn std::error::Error>>;

fn main() -> BenchResult<()> {
	let mut args = std::env::args().skip(1);
	let mut base: Option<PathBuf> = None;
	let mut source: Option<PathBuf> = None;

	while let Some(arg) = args.next() {
		match arg.as_str() {
			"--dir" => {
				if let Some(p) = args.next() {
					base = Some(PathBuf::from(p));
				}
			},
			"--source" => {
				if let Some(p) = args.next() {
					source = Some(PathBuf::from(p));
				}
			},
			_ => {},
		}
	}

	let source = source.unwrap_or_else(|| std::env::temp_dir().join(Path::new("fjall_bench/index")));
	let base = base.unwrap_or_else(|| std::env::temp_dir().join(Path::new("fst_txhash_bench")));
	std::fs::create_dir_all(&base)?;

	let out_path = base.join("txhash_from_fjall.fst");
	run_from_fjall(&source, &out_path)?;
	println!("written {}", out_path.display());

	Ok(())
}

fn run_from_fjall(source: &Path, out: &Path) -> BenchResult<()> {
	let keyspace = Config::new(source).open()?;
	let partition = keyspace.open_partition("col1", PartitionCreateOptions::default())?;

	let file = File::create(out)?;
	let writer = BufWriter::new(file);
	let mut builder = MapBuilder::new(writer)?;
	let mut tracker = ProgressTracker::new("fst-from-fjall".to_string(), 0);

	let mut count = 0u64;
	let mut last: Option<Vec<u8>> = None;
	for kv in partition.range::<&[u8], _>(..) {
		let (k, _) = kv?;
		let key_bytes = k.as_ref();
		if last.as_deref() == Some(key_bytes) {
			continue;
		}
		if count == 0 {
			println!("first key (hex): {}", hex::encode(key_bytes));
		}
		builder.insert(key_bytes, count)?;
		count += 1;
		tracker.record(1);
		last = Some(key_bytes.to_vec());
	}

	println!("ingested {count} keys from {}", source.display());
	builder.finish()?;
	Ok(())
}
