mod parity_store;

use bech32::{ToBase32, Variant};
use bs58;
use rand::{rngs::StdRng, RngCore, SeedableRng};
use std::{
	num::NonZeroUsize,
	path::Path,
	sync::{
		atomic::{AtomicU64, Ordering},
		Arc,
	},
	thread,
	time::Instant,
};
use std::path::PathBuf;
use crossbeam_channel::bounded;

use parity_store::{Layout, Store, StoreCodec, StoreResult};

const KEY_LEN: usize = 16; // u64 + u32 + u32
const BATCH: usize = 20_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Key16(pub [u8; KEY_LEN]);

impl Key16 {
	pub fn from_fields(a: u64, b: u32, c: u32) -> Self {
		let mut out = [0u8; KEY_LEN];
		out[0..8].copy_from_slice(&a.to_le_bytes());
		out[8..12].copy_from_slice(&b.to_le_bytes());
		out[12..16].copy_from_slice(&c.to_le_bytes());
		Self(out)
	}
}
impl AsRef<[u8]> for Key16 {
	fn as_ref(&self) -> &[u8] {
		&self.0
	}
}

#[derive(Clone, Copy, Debug)]
pub struct Amount(pub u64);

#[derive(Clone, Debug)]
pub struct TxHash(pub [u8; 32]);
impl AsRef<[u8]> for TxHash {
	fn as_ref(&self) -> &[u8] {
		&self.0
	}
}

#[derive(Clone, Copy, Debug)]
pub struct Timestamp(pub u64);

#[derive(Clone, Debug)]
pub struct Address(pub Vec<u8>);
impl AsRef<[u8]> for Address {
	fn as_ref(&self) -> &[u8] {
		&self.0
	}
}

struct KeyCodec;
struct AmountCodec;
struct TxCodec;
struct TimestampCodec;
struct AddressCodec;

impl StoreCodec<Key16> for KeyCodec {
	type Enc<'a> = &'a [u8];
	fn encode<'a>(value: &'a Key16) -> Self::Enc<'a> {
		value.as_ref()
	}
	fn decode(bytes: &[u8]) -> StoreResult<Key16> {
		let arr: [u8; KEY_LEN] =
			bytes.try_into().map_err(|_| parity_db::Error::InvalidInput("bad key length".into()))?;
		Ok(Key16(arr))
	}
}

impl StoreCodec<Amount> for AmountCodec {
	type Enc<'a> = [u8; 8];
	fn encode<'a>(value: &'a Amount) -> Self::Enc<'a> {
		value.0.to_le_bytes()
	}
	fn decode(bytes: &[u8]) -> StoreResult<Amount> {
		let arr: [u8; 8] =
			bytes.try_into().map_err(|_| parity_db::Error::InvalidInput("bad amount".into()))?;
		Ok(Amount(u64::from_le_bytes(arr)))
	}
}

impl StoreCodec<TxHash> for TxCodec {
	type Enc<'a> = &'a [u8];
	fn encode<'a>(value: &'a TxHash) -> Self::Enc<'a> {
		value.as_ref()
	}
	fn decode(bytes: &[u8]) -> StoreResult<TxHash> {
		let arr: [u8; 32] =
			bytes.try_into().map_err(|_| parity_db::Error::InvalidInput("bad tx hash".into()))?;
		Ok(TxHash(arr))
	}
}

impl StoreCodec<Timestamp> for TimestampCodec {
	type Enc<'a> = [u8; 8];
	fn encode<'a>(value: &'a Timestamp) -> Self::Enc<'a> {
		value.0.to_le_bytes()
	}
	fn decode(bytes: &[u8]) -> StoreResult<Timestamp> {
		let arr: [u8; 8] =
			bytes.try_into().map_err(|_| parity_db::Error::InvalidInput("bad timestamp".into()))?;
		Ok(Timestamp(u64::from_le_bytes(arr)))
	}
}

impl StoreCodec<Address> for AddressCodec {
	type Enc<'a> = &'a [u8];
	fn encode<'a>(value: &'a Address) -> Self::Enc<'a> {
		value.as_ref()
	}
	fn decode(bytes: &[u8]) -> StoreResult<Address> {
		Ok(Address(bytes.to_vec()))
	}
}

fn main() -> StoreResult<()> {
    let tmp_dir = std::env::temp_dir();
	let base = tmp_dir.join(Path::new("parity_bench"));
	let total = 50_000_000u64;

	// Clean previous runs for fair benchmarks.
	for dir in ["plain", "index", "range", "dictionary"] {
		let path = base.join(dir);
		if path.exists() {
			std::fs::remove_dir_all(&path).ok();
		}
	}

	run_all_parallel(&base, total)?;

	Ok(())
}

fn run_plain(base: &Path, total: u64) -> StoreResult<()> {
	let path = base.join("plain");
	let store = Store::<Key16, Amount, KeyCodec, AmountCodec>::open(&path, Layout::plain(0))?;
	let start = Instant::now();
	let mut last_report = start;
	let mut inserted: u64 = 0;
	let mut batch: Vec<(Key16, Amount)> = Vec::with_capacity(BATCH);
	for i in 0..total {
		batch.push((make_key(i), Amount(i)));
		if batch.len() >= BATCH {
			store.commit(batch.iter().map(|(k, v)| (k, v)))?;
			batch.clear();
			inserted += BATCH as u64;
			maybe_report("plain", inserted, total, start, &mut last_report);
		}
	}
	if !batch.is_empty() {
		store.commit(batch.iter().map(|(k, v)| (k, v)))?;
		inserted += batch.len() as u64;
		maybe_report("plain", inserted, total, start, &mut last_report);
	}
	let elapsed = start.elapsed();
	println!("plain: wrote {} in {:.2?} (~{:.1} ops/s)", total, elapsed, ops_per_sec(total, elapsed));
	Ok(())
}

fn run_index(base: &Path, total: u64) -> StoreResult<()> {
	let path = base.join("index");
	let store = Store::<Key16, TxHash, KeyCodec, TxCodec>::open(&path, Layout::unique_index(0))?;
	let mut rng = StdRng::seed_from_u64(1);
	let start = Instant::now();
	let mut last_report = start;
	let mut inserted: u64 = 0;
	let mut batch: Vec<(Key16, TxHash)> = Vec::with_capacity(BATCH);
	for i in 0..total {
		let k = make_key(i);
		let mut h = [0u8; 32];
		rng.fill_bytes(&mut h);
		batch.push((k, TxHash(h)));
		if batch.len() >= BATCH {
			store.commit(batch.iter().map(|(k, v)| (k, v)))?;
			batch.clear();
			inserted += BATCH as u64;
			maybe_report("index", inserted, total, start, &mut last_report);
		}
	}
	if !batch.is_empty() {
		store.commit(batch.iter().map(|(k, v)| (k, v)))?;
		inserted += batch.len() as u64;
		maybe_report("index", inserted, total, start, &mut last_report);
	}
	let elapsed = start.elapsed();
	println!("index: wrote {} in {:.2?} (~{:.1} ops/s)", total, elapsed, ops_per_sec(total, elapsed));
	Ok(())
}

fn run_range(base: &Path, total: u64) -> StoreResult<()> {
	let path = base.join("range");
	let store =
		Store::<Key16, Timestamp, KeyCodec, TimestampCodec>::open(&path, Layout::range(0))?;
	let start = Instant::now();
	let mut last_report = start;
	let mut inserted: u64 = 0;
	let mut batch: Vec<(Key16, Timestamp)> = Vec::with_capacity(BATCH);
	for i in 0..total {
		batch.push((make_key(i), Timestamp(i)));
		if batch.len() >= BATCH {
			store.commit(batch.iter().map(|(k, v)| (k, v)))?;
			batch.clear();
			inserted += BATCH as u64;
			maybe_report("range", inserted, total, start, &mut last_report);
		}
	}
	if !batch.is_empty() {
		store.commit(batch.iter().map(|(k, v)| (k, v)))?;
		inserted += batch.len() as u64;
		maybe_report("range", inserted, total, start, &mut last_report);
	}
	let elapsed = start.elapsed();
	println!("range: wrote {} in {:.2?} (~{:.1} ops/s)", total, elapsed, ops_per_sec(total, elapsed));
	Ok(())
}

fn run_dictionary(base: &Path, total: u64) -> StoreResult<()> {
	let path = base.join("dictionary");
	let store = Store::<Key16, Address, KeyCodec, AddressCodec>::open(
		&path,
		Layout::dictionary(0),
	)?;
	let mut stream = AddressStream::new(total, 2);
	let mut seen_addr: Option<Address> = None;
	let start = Instant::now();
	let mut last_report = start;
	let mut inserted: u64 = 0;
	let mut batch: Vec<(Key16, Address)> = Vec::with_capacity(BATCH);
	for i in 0..total {
		if let Some(v) = stream.next() {
			let k = make_key(i);
			if i % 5 == 0 {
				seen_addr = Some(v.clone());
			}
			batch.push((k, v));
		}
		if batch.len() >= BATCH {
			store.commit(batch.iter().map(|(k, v)| (k, v)))?;
			batch.clear();
			inserted += BATCH as u64;
			maybe_report("dictionary", inserted, total, start, &mut last_report);
		}
	}
	if !batch.is_empty() {
		store.commit(batch.iter().map(|(k, v)| (k, v)))?;
		inserted += batch.len() as u64;
		maybe_report("dictionary", inserted, total, start, &mut last_report);
	}
	stream.join();
	let elapsed = start.elapsed();
	println!(
		"dictionary: wrote {} in {:.2?} (~{:.1} ops/s)",
		total,
		elapsed,
		ops_per_sec(total, elapsed)
	);

	// quick sanity reverse lookup
	if let Some(addr) = seen_addr {
		let keys = store.get_keys_for_value(&addr)?;
	println!("dictionary: sample reverse lookup keys count = {}", keys.len());
	}
	Ok(())
}

fn run_all_parallel(base: &PathBuf, total: u64) -> StoreResult<()> {
	use std::thread;
	let handles = vec![
		thread::spawn({
			let base = base.clone();
			move || run_plain(&base, total)
		}),
		thread::spawn({
			let base = base.clone();
			move || run_index(&base, total)
		}),
		thread::spawn({
			let base = base.clone();
			move || run_range(&base, total)
		}),
		thread::spawn({
			let base = base.clone();
			move || run_dictionary(&base, total)
		}),
	];
	for h in handles {
		h.join().unwrap()?;
	}
	Ok(())
}

fn make_key(i: u64) -> Key16 {
	let a = i;
	let b = (i as u32).wrapping_mul(17);
	let c = (i as u32).wrapping_mul(31);
	Key16::from_fields(a, b, c)
}

fn ops_per_sec(total: u64, elapsed: std::time::Duration) -> f64 {
	total as f64 / elapsed.as_secs_f64()
}

fn maybe_report(label: &str, inserted: u64, total: u64, start: Instant, last_report: &mut Instant) {
	let now = Instant::now();
	if now.duration_since(*last_report).as_secs() >= 5 {
		let elapsed = now.duration_since(start);
		let speed = ops_per_sec(inserted, elapsed);
		println!("{label}: progress {inserted}/{total} (~{:.1} ops/s)", speed);
		*last_report = now;
	}
}

fn random_address(rng: &mut StdRng) -> Address {
	if rng.next_u32() & 1 == 0 {
		base58_address(rng)
	} else {
		bech32_address(rng)
	}
}

fn base58_address(rng: &mut StdRng) -> Address {
	let version = if rng.next_u32() & 1 == 0 { 0x00 } else { 0x05 }; // P2PKH / P2SH
	let mut payload = [0u8; 20];
	rng.fill_bytes(&mut payload);
	let mut data = Vec::with_capacity(1 + payload.len());
	data.push(version);
	data.extend_from_slice(&payload);
	Address(bs58::encode(data).into_string().into_bytes())
}

fn bech32_address(rng: &mut StdRng) -> Address {
	let taproot = rng.next_u32() & 1 == 0;
	let (version, len, variant) =
		if taproot { (1u8, 32usize, Variant::Bech32m) } else { (0u8, 20usize, Variant::Bech32) };
	let mut program = vec![0u8; len];
	rng.fill_bytes(&mut program);

	let mut data = Vec::with_capacity(1 + program.len());
	data.push(bech32::u5::try_from_u8(version).expect("valid witness version"));
	data.extend(program.to_base32());

	let addr = bech32::encode("bc", data, variant).expect("encode succeeds");
	Address(addr.into_bytes())
}

struct AddressStream {
	rx: crossbeam_channel::Receiver<Address>,
	handles: Vec<thread::JoinHandle<()>>,
}

impl AddressStream {
	fn new(total: u64, seed: u64) -> Self {
		let (tx, rx) = bounded(1024);
		let counter = Arc::new(AtomicU64::new(0));
		let threads = thread::available_parallelism().unwrap_or(NonZeroUsize::new(1).unwrap()).get();
		let mut handles = Vec::new();
		for t in 0..threads {
			let tx = tx.clone();
			let counter = counter.clone();
			handles.push(thread::spawn(move || {
				let mut rng = StdRng::seed_from_u64(seed + t as u64);
				let mut last: Option<Address> = None;
				loop {
					let idx = counter.fetch_add(1, Ordering::Relaxed);
					if idx >= total {
						break
					}
					let addr = if idx % 5 == 0 {
						let a = random_address(&mut rng);
						last = Some(a.clone());
						a
					} else {
						last.clone().unwrap_or_else(|| {
							let a = random_address(&mut rng);
							last = Some(a.clone());
							a
						})
					};
					if tx.send(addr).is_err() {
						break
					}
				}
			}));
		}
		drop(tx);
		Self { rx, handles }
	}

	fn join(self) {
		for h in self.handles {
			let _ = h.join();
		}
	}
}

impl Iterator for AddressStream {
	type Item = Address;
	fn next(&mut self) -> Option<Self::Item> {
		self.rx.recv().ok()
	}
}
