use crate::store_interface::StoreWrite;
use bech32::{ToBase32, Variant};
use bs58;
use crossbeam_channel::bounded;
use rand::{rngs::StdRng, RngCore, SeedableRng};
use std::{
	num::NonZeroUsize,
	path::Path,
	sync::{
		atomic::{AtomicU64, Ordering},
		Arc,
	},
	thread,
};

pub const BATCH: usize = 20_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Key(pub u64);

impl Key {
	pub fn to_be_bytes(self) -> [u8; 8] {
		self.0.to_be_bytes()
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

pub fn run_plain<S, F>(base: &Path, total: u64, factory: F) -> Result<(), S::Error>
where
	S: StoreWrite<Key, Amount>,
	F: Fn(&Path) -> Result<S, S::Error>,
{
	let path = base.join("plain");
	let mut store = factory(&path)?;
	store.set_progress("plain", total);
	let mut _inserted: u64 = 0;
	let mut batch: Vec<(Key, Amount)> = Vec::with_capacity(BATCH);
	for i in 0..total {
		batch.push((make_key(i), Amount(i)));
		if batch.len() >= BATCH {
			store.commit(batch.iter().map(|(k, v)| (k, v)))?;
			batch.clear();
			_inserted += BATCH as u64;
		}
	}
	if !batch.is_empty() {
		store.commit(batch.iter().map(|(k, v)| (k, v)))?;
		_inserted += batch.len() as u64;
	}
	store.flush()?;
	Ok(())
}

pub fn run_index<S, F>(base: &Path, total: u64, factory: F) -> Result<(), S::Error>
where
	S: StoreWrite<Key, TxHash>,
	F: Fn(&Path) -> Result<S, S::Error>,
{
	let path = base.join("index");
	let mut store = factory(&path)?;
	store.set_progress("index", total);
	let mut rng = StdRng::seed_from_u64(1);
	let mut _inserted: u64 = 0;
	let mut batch: Vec<(Key, TxHash)> = Vec::with_capacity(BATCH);
	for i in 0..total {
		let k = make_key(i);
		let mut h = [0u8; 32];
		rng.fill_bytes(&mut h);
		batch.push((k, TxHash(h)));
		if batch.len() >= BATCH {
			store.commit(batch.iter().map(|(k, v)| (k, v)))?;
			batch.clear();
			_inserted += BATCH as u64;
		}
	}
	if !batch.is_empty() {
		store.commit(batch.iter().map(|(k, v)| (k, v)))?;
		_inserted += batch.len() as u64;
	}
	store.flush()?;
	Ok(())
}

pub fn run_range<S, F>(base: &Path, total: u64, factory: F) -> Result<(), S::Error>
where
	S: StoreWrite<Key, Timestamp>,
	F: Fn(&Path) -> Result<S, S::Error>,
{
	let path = base.join("range");
	let mut store = factory(&path)?;
	store.set_progress("range", total);
	let mut _inserted: u64 = 0;
	let mut batch: Vec<(Key, Timestamp)> = Vec::with_capacity(BATCH);
	for i in 0..total {
		batch.push((make_key(i), Timestamp(i)));
		if batch.len() >= BATCH {
			store.commit(batch.iter().map(|(k, v)| (k, v)))?;
			batch.clear();
			_inserted += BATCH as u64;
		}
	}
	if !batch.is_empty() {
		store.commit(batch.iter().map(|(k, v)| (k, v)))?;
		_inserted += batch.len() as u64;
	}
	store.flush()?;
	Ok(())
}

pub fn run_dictionary<S, F>(base: &Path, total: u64, factory: F) -> Result<(), S::Error>
where
	S: StoreWrite<Key, Address>,
	F: Fn(&Path) -> Result<S, S::Error>,
{
	let path = base.join("dictionary");
	let mut store = factory(&path)?;
	store.set_progress("dictionary", total);
	let mut stream = AddressStream::new(total, 2);
	let mut seen_addr: Option<Address> = None;
	let mut _inserted: u64 = 0;
	let mut batch: Vec<(Key, Address)> = Vec::with_capacity(BATCH);
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
			_inserted += BATCH as u64;
		}
	}
	if !batch.is_empty() {
		store.commit(batch.iter().map(|(k, v)| (k, v)))?;
		_inserted += batch.len() as u64;
	}
	stream.join();
	store.flush()?;
	if let Some(addr) = seen_addr {
		let keys = store.get_keys_for_value(&addr)?;
	}
	Ok(())
}

pub fn run_all_parallel<E>(jobs: Vec<Box<dyn FnOnce() -> Result<(), E> + Send>>) -> Result<(), E>
where
	E: Send + 'static,
{
	let handles = jobs.into_iter().map(|job| thread::spawn(job)).collect::<Vec<_>>();
	for h in handles {
		h.join().unwrap()?;
	}
	Ok(())
}

pub fn cleanup_dirs(base: &Path, dirs: &[&str]) {
	for dir in dirs {
		let path = base.join(dir);
		if path.exists() {
			std::fs::remove_dir_all(&path).ok();
		}
	}
}

pub fn make_key(i: u64) -> Key {
	Key(i)
}

pub fn ops_per_sec(total: u64, elapsed: std::time::Duration) -> f64 {
	total as f64 / elapsed.as_secs_f64()
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

pub struct AddressStream {
	rx: crossbeam_channel::Receiver<Address>,
	handles: Vec<thread::JoinHandle<()>>,
}

impl AddressStream {
	pub fn new(total: u64, seed: u64) -> Self {
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

	pub fn join(self) {
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
