use crate::store_interface::{ProgressTracker, StoreRead, StoreWrite};
use std::{fs, io, marker::PhantomData, path::Path, sync::{Arc, RwLock}};

pub type StoreResult<T> = Result<T, StoreError>;
use crate::fst::compactor::Compactor;
use crate::fst::segment::Column;
pub use crate::store_interface::StoreCodec;

#[derive(Debug)]
pub enum StoreError {
	Io(io::Error),
	Fst(fst::Error),
	InvalidInput(String),
	CorruptSegment(String),
}

impl std::fmt::Display for StoreError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			StoreError::Io(err) => write!(f, "io error: {err}"),
			StoreError::Fst(err) => write!(f, "fst error: {err}"),
			StoreError::InvalidInput(msg) => write!(f, "invalid input: {msg}"),
			StoreError::CorruptSegment(msg) => write!(f, "corrupt segment: {msg}"),
		}
	}
}

impl std::error::Error for StoreError {}

impl From<io::Error> for StoreError {
	fn from(err: io::Error) -> Self {
		StoreError::Io(err)
	}
}

impl From<fst::Error> for StoreError {
	fn from(err: fst::Error) -> Self {
		StoreError::Fst(err)
	}
}

#[derive(Clone, Copy, Debug)]
pub enum Layout {
	Plain { key_to_value: u8 },
	UniqueIndex { key_to_value: u8, value_to_key: u8 },
	Range { key_to_value: u8, value_key_btree: u8 },
	Dictionary { key_to_birth_key: u8, birth_key_to_value: u8, value_to_birth_key: u8, birth_key_key_btree: u8 },
}

impl Layout {
	pub fn plain(from: u8) -> Layout {
		Layout::Plain { key_to_value: from }
	}
	pub fn unique_index(from: u8) -> Layout {
		Layout::UniqueIndex { key_to_value: from, value_to_key: from + 1 }
	}
	pub fn range(from: u8) -> Layout {
		Layout::Range { key_to_value: from, value_key_btree: from + 1 }
	}
	pub fn dictionary(from: u8) -> Layout {
		Layout::Dictionary {
			key_to_birth_key: from,
			birth_key_to_value: from + 1,
			value_to_birth_key: from + 2,
			birth_key_key_btree: from + 3,
		}
	}

	fn column_count(&self) -> usize {
		match self {
			Layout::Plain { .. } => 1,
			Layout::UniqueIndex { .. } => 2,
			Layout::Range { .. } => 2,
			Layout::Dictionary { .. } => 4,
		}
	}
}

#[derive(Clone, Copy)]
pub struct StoreOptions {
	pub segment_size: usize,
}

impl Default for StoreOptions {
	fn default() -> Self {
		Self { segment_size: MIN_SEGMENT_ROWS }
	}
}

impl StoreOptions {
	pub fn new(segment_size: usize) -> Self {
		Self { segment_size }
	}

	pub fn from_estimates(approx_rows: u64, avg_kv_bytes: usize, mem_budget_bytes: usize) -> Self {
		let segment_size = compute_segment_size(approx_rows, avg_kv_bytes, mem_budget_bytes);
		Self { segment_size }
	}
}

pub struct Store<K, V, KC, VC>
where
	KC: StoreCodec<K, Error = StoreError>,
	VC: StoreCodec<V, Error = StoreError>,
{
	layout: Layout,
	columns: Vec<Arc<RwLock<Column>>>,
	compactor: Compactor,
	progress: Option<ProgressTracker>,
	_ph: PhantomData<(K, V, KC, VC)>,
}

impl<K, V, KC, VC> Store<K, V, KC, VC>
where
	KC: StoreCodec<K, Error = StoreError>,
	VC: StoreCodec<V, Error = StoreError>,
{
	pub fn open(path: &Path, layout: Layout, options: StoreOptions) -> StoreResult<Self> {
		if options.segment_size == 0 {
			return Err(StoreError::InvalidInput("segment_size must be > 0".into()))
		}
		if !path.exists() {
			fs::create_dir_all(path)?;
		}
		let mut columns = Vec::new();
		for idx in 0..layout.column_count() {
			let col = Column::open(path, idx as u8, options.segment_size)?;
			columns.push(Arc::new(RwLock::new(col)));
		}
		let compactor = Compactor::new(columns.clone());
		Ok(Self { layout, columns, compactor, progress: None, _ph: PhantomData })
	}

	pub fn commit<'a, I>(&mut self, items: I) -> StoreResult<()>
	where I: IntoIterator<Item = (&'a K, &'a V)>, K: 'a, V: 'a,
	{
		let mut processed = 0u64;
		match self.layout {
			Layout::Plain { key_to_value } => {
				for (k, v) in items {
					let kbytes = KC::encode(k);
					let vbytes = VC::encode(v);
					let flushed = self.columns[key_to_value as usize].write().unwrap().insert(kbytes.as_ref().to_vec(), vbytes.as_ref().to_vec())?;
					if flushed {
						self.compactor.request(key_to_value as usize)?;
					}
					processed += 1;
				}
			},
			Layout::UniqueIndex { key_to_value, value_to_key } => {
				for (k, v) in items {
					let kbytes = KC::encode(k);
					let vbytes = VC::encode(v);
					let kvec = kbytes.as_ref().to_vec();
					let vvec = vbytes.as_ref().to_vec();
					let flushed1 = self.columns[key_to_value as usize].write().unwrap().insert(kvec.clone(), vvec.clone())?;
					let flushed2 = self.columns[value_to_key as usize].write().unwrap().insert(vvec, kvec)?;
					if flushed1 {
						self.compactor.request(key_to_value as usize)?;
					}
					if flushed2 {
						self.compactor.request(value_to_key as usize)?;
					}
					processed += 2;
				}
			},
			Layout::Range { key_to_value, value_key_btree } => {
				for (k, v) in items {
					let kbytes = KC::encode(k);
					let vbytes = VC::encode(v);
					let kvec = kbytes.as_ref().to_vec();
					let vvec = vbytes.as_ref().to_vec();
					let vk = concat(vbytes.as_ref(), kbytes.as_ref());
					let flushed1 = self.columns[key_to_value as usize].write().unwrap().insert(kvec, vvec)?;
					let flushed2 = self.columns[value_key_btree as usize].write().unwrap().insert(vk, Vec::new())?;
					if flushed1 {
						self.compactor.request(key_to_value as usize)?;
					}
					if flushed2 {
						self.compactor.request(value_key_btree as usize)?;
					}
					processed += 2;
				}
			},
			Layout::Dictionary { key_to_birth_key, birth_key_to_value, value_to_birth_key, birth_key_key_btree } => {
				use std::collections::HashMap;
				let mut value_cache: HashMap<Vec<u8>, (Vec<u8>, bool)> = HashMap::new();
				for (k, v) in items {
					let kbytes = KC::encode(k);
					let vbytes = VC::encode(v);
					let kvec = kbytes.as_ref().to_vec();
					let vvec = vbytes.as_ref().to_vec();
					let (pk, is_new) = if let Some(entry) = value_cache.get(&vvec) {
						entry.clone()
					} else if let Some(pk) = self.columns[value_to_birth_key as usize].read().unwrap().get(&vvec)? {
						value_cache.insert(vvec.clone(), (pk.clone(), false));
						(pk, false)
					} else {
						value_cache.insert(vvec.clone(), (kvec.clone(), true));
						(kvec.clone(), true)
					};

					if is_new {
                        processed += 2;
						let flushed_v2b = self.columns[value_to_birth_key as usize].write().unwrap().insert(vvec.clone(), pk.clone())?;
						let flushed_b2v = self.columns[birth_key_to_value as usize].write().unwrap().insert(pk.clone(), vvec.clone())?;
						if flushed_v2b {
							self.compactor.request(value_to_birth_key as usize)?;
						}
						if flushed_b2v {
							self.compactor.request(birth_key_to_value as usize)?;
						}
					}
					let flushed_k2b = self.columns[key_to_birth_key as usize].write().unwrap().insert(kvec.clone(), pk.clone())?;
					if flushed_k2b {
						self.compactor.request(key_to_birth_key as usize)?;
					}

					let pk_key = concat(&pk, &kvec);
					let flushed_btree = self.columns[birth_key_key_btree as usize].write().unwrap().insert(pk_key, Vec::new())?;
					if flushed_btree {
						self.compactor.request(birth_key_key_btree as usize)?;
					}
					processed += 2;
				}
			},
		}
		if let Some(p) = self.progress.as_mut() {
			p.record(processed);
		}
		Ok(())
	}

	pub fn flush(&mut self) -> StoreResult<()> {
		for col in &self.columns {
			col.write().unwrap().flush()?;
		}
		Ok(())
	}

	pub fn multi_way_merge(&mut self) -> StoreResult<()> {
		for col in &self.columns {
			col.write().unwrap().multi_way_merge()?;
		}
		Ok(())
	}

	pub fn get_value(&self, key: &K) -> StoreResult<Option<V>> {
		let kbytes = KC::encode(key);
		match self.layout {
			Layout::Plain { key_to_value }
			| Layout::UniqueIndex { key_to_value, .. }
			| Layout::Range { key_to_value, .. } => {
				self.columns[key_to_value as usize]
					.read().unwrap()
					.get(kbytes.as_ref())
					.map(|opt| opt.map(|b| VC::decode(&b)).transpose())?
			},
			Layout::Dictionary { key_to_birth_key, birth_key_to_value, .. } => {
				if let Some(pk) = self.columns[key_to_birth_key as usize].read().unwrap().get(kbytes.as_ref())? {
					self.columns[birth_key_to_value as usize]
						.read().unwrap()
						.get(&pk)
						.map(|opt| opt.map(|b| VC::decode(&b)).transpose())?
				} else {
					Ok(None)
				}
			},
		}
	}

	pub fn get_key_for_value(&self, value: &V) -> StoreResult<Option<K>> {
		let vbytes = VC::encode(value);
		match self.layout {
			Layout::UniqueIndex { value_to_key, .. } => {
				self.columns[value_to_key as usize]
					.read().unwrap()
					.get(vbytes.as_ref())
					.map(|opt| opt.map(|b| KC::decode(&b)).transpose())?
			},
			_ => Err(StoreError::InvalidInput("get_key_for_value not supported for this layout".into())),
		}
	}

	pub fn get_keys_for_value(&self, value: &V) -> StoreResult<Vec<K>> {
		let vbytes = VC::encode(value);
		match self.layout {
			Layout::Range { value_key_btree, .. } => {
				let prefix = vbytes.as_ref();
				let keys = self.columns[value_key_btree as usize].read().unwrap().keys_with_prefix(prefix)?;
				let mut out = Vec::new();
				for k in keys {
					if k.len() < prefix.len() {
						continue
					}
					let key_bytes = &k[prefix.len()..];
					out.push(KC::decode(key_bytes)?);
				}
				Ok(out)
			},
			Layout::Dictionary { value_to_birth_key, birth_key_key_btree, .. } => {
				if let Some(pk) = self.columns[value_to_birth_key as usize].read().unwrap().get(vbytes.as_ref())? {
					let keys = self.columns[birth_key_key_btree as usize].read().unwrap().keys_with_prefix(&pk)?;
					let mut out = Vec::new();
					for k in keys {
						if k.len() < pk.len() {
							continue
						}
						let suffix = &k[pk.len()..];
						out.push(KC::decode(suffix)?);
					}
					Ok(out)
				} else {
					Ok(Vec::new())
				}
			},
			_ => Err(StoreError::InvalidInput("get_keys_for_value not supported for this layout".into())),
		}
	}
}

fn concat(a: &[u8], b: &[u8]) -> Vec<u8> {
	let mut out = Vec::with_capacity(a.len() + b.len());
	out.extend_from_slice(a);
	out.extend_from_slice(b);
	out
}

impl<K, V, KC, VC> StoreRead<K, V> for Store<K, V, KC, VC>
where
	KC: StoreCodec<K, Error = StoreError>,
	VC: StoreCodec<V, Error = StoreError>,
{
	type Error = StoreError;

	fn get_value(&self, key: &K) -> StoreResult<Option<V>> {
		Store::get_value(self, key)
	}

	fn get_key_for_value(&self, value: &V) -> StoreResult<Option<K>> {
		Store::get_key_for_value(self, value)
	}

	fn get_keys_for_value(&self, value: &V) -> StoreResult<Vec<K>> {
		Store::get_keys_for_value(self, value)
	}
}

impl<K, V, KC, VC> StoreWrite<K, V> for Store<K, V, KC, VC>
where
	KC: StoreCodec<K, Error = StoreError>,
	VC: StoreCodec<V, Error = StoreError>,
{
	type Options = StoreOptions;
	type Layout = Layout;

	fn open_with_options(path: &Path, layout: Self::Layout, options: Self::Options) -> StoreResult<Self> {
		Store::open(path, layout, options)
	}

	fn commit<'a, I>(&mut self, items: I) -> StoreResult<()>
	where
		I: IntoIterator<Item = (&'a K, &'a V)>,
		K: 'a,
		V: 'a,
	{
		Store::commit(self, items)
	}

	fn flush(&mut self) -> StoreResult<()> {
		Store::flush(self)
	}

	fn set_progress(&mut self, label: &str, total: u64) {
		self.progress = Some(ProgressTracker::new(label.to_string(), total));
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::store_tests::{basic_value_roundtrip, multiple_keys_for_value, reverse_lookup_unique};
	use tempfile::tempdir;

	struct BytesCodec;

	impl StoreCodec<Vec<u8>> for BytesCodec {
		type Error = StoreError;
		type Enc<'a> = &'a [u8];
		fn encode<'a>(value: &'a Vec<u8>) -> Self::Enc<'a> {
			value.as_slice()
		}
		fn decode(bytes: &[u8]) -> StoreResult<Vec<u8>> {
			Ok(bytes.to_vec())
		}
	}

	#[test]
	fn writes_and_reads_from_memtable() {
		let dir = tempdir().unwrap();
		let mut store =
			Store::<Vec<u8>, Vec<u8>, BytesCodec, BytesCodec>::open(dir.path(), Layout::plain(0), StoreOptions { segment_size: 10 }).unwrap();

		store.commit([(&b"key"[..].to_vec(), &b"value"[..].to_vec())]).unwrap();
		let got = store.get_value(&b"key"[..].to_vec()).unwrap();
		assert_eq!(got, Some(b"value".to_vec()));
		assert!(store.columns[0].read().unwrap().segments.is_empty(), "no segment should be flushed yet");
	}

	#[test]
	fn flushes_to_segment_and_recovers() {
		let dir = tempdir().unwrap();
		{
			let mut store =
				Store::<Vec<u8>, Vec<u8>, BytesCodec, BytesCodec>::open(dir.path(), Layout::plain(0), StoreOptions { segment_size: 2 }).unwrap();
			store.commit([
				(&b"a"[..].to_vec(), &b"1"[..].to_vec()),
				(&b"b"[..].to_vec(), &b"2"[..].to_vec()),
			]).unwrap();
			store.flush().unwrap();
			assert_eq!(store.columns[0].read().unwrap().segments.len(), 1);
		}

		let store =
			Store::<Vec<u8>, Vec<u8>, BytesCodec, BytesCodec>::open(dir.path(), Layout::plain(0), StoreOptions { segment_size: 2 }).unwrap();
		assert_eq!(store.get_value(&b"a"[..].to_vec()).unwrap(), Some(b"1".to_vec()));
		assert_eq!(store.get_value(&b"b"[..].to_vec()).unwrap(), Some(b"2".to_vec()));
	}

	#[test]
	fn picks_latest_value_across_segments() {
		let dir = tempdir().unwrap();
		let mut store =
			Store::<Vec<u8>, Vec<u8>, BytesCodec, BytesCodec>::open(dir.path(), Layout::plain(0), StoreOptions { segment_size: 1 }).unwrap();
		store.commit([(&b"k"[..].to_vec(), &b"old"[..].to_vec())]).unwrap();
		store.flush().unwrap();
		store.commit([(&b"k"[..].to_vec(), &b"new"[..].to_vec())]).unwrap();
		assert_eq!(store.get_value(&b"k"[..].to_vec()).unwrap(), Some(b"new".to_vec()));
		store.flush().unwrap();
		assert_eq!(store.get_value(&b"k"[..].to_vec()).unwrap(), Some(b"new".to_vec()));
	}

	#[test]
	fn range_lookup_deduplicates() {
		let dir = tempdir().unwrap();
		let mut store =
			Store::<Vec<u8>, Vec<u8>, BytesCodec, BytesCodec>::open(dir.path(), Layout::range(0), StoreOptions { segment_size: 2 }).unwrap();
		let entries = [
			(&b"k1"[..].to_vec(), &b"v1"[..].to_vec()),
			(&b"k2"[..].to_vec(), &b"v1"[..].to_vec()),
		];
		store.commit(entries).unwrap();
		store.flush().unwrap();
		store.commit([(&b"k3"[..].to_vec(), &b"v1"[..].to_vec())]).unwrap();

		let keys = store.get_keys_for_value(&b"v1"[..].to_vec()).unwrap();
		let mut sorted = keys;
		sorted.sort();
		assert_eq!(sorted, vec![b"k1".to_vec(), b"k2".to_vec(), b"k3".to_vec()]);
	}

	#[test]
	fn shared_basic_suite() {
		let options = StoreOptions { segment_size: 3 };
		basic_value_roundtrip(|| {
			let dir = tempdir().unwrap();
			let path = dir.path().to_path_buf();
			std::mem::forget(dir);
			Store::<Vec<u8>, Vec<u8>, BytesCodec, BytesCodec>::open(&path, Layout::plain(0), options).unwrap()
		});
	}

	#[test]
	fn shared_reverse_suite() {
		let options = StoreOptions { segment_size: 2 };
		reverse_lookup_unique(|| {
			let dir = tempdir().unwrap();
			let path = dir.path().to_path_buf();
			std::mem::forget(dir);
			Store::<Vec<u8>, Vec<u8>, BytesCodec, BytesCodec>::open(&path, Layout::unique_index(0), options).unwrap()
		});
	}

	#[test]
	fn shared_multiple_keys_suite() {
		let options = StoreOptions { segment_size: 2 };
		multiple_keys_for_value(|| {
			let dir = tempdir().unwrap();
			let path = dir.path().to_path_buf();
			std::mem::forget(dir);
			Store::<Vec<u8>, Vec<u8>, BytesCodec, BytesCodec>::open(&path, Layout::range(0), options).unwrap()
		});
	}

	#[test]
	fn sizing_respects_min_and_target_segments() {
		let size = compute_segment_size(10_000_000, 32, DEFAULT_MEMTABLE_BUDGET_BYTES);
		assert_eq!(size, 312_500);
	}

	#[test]
	fn sizing_clamps_to_memtable_budget() {
		let size = compute_segment_size(1_000_000, 1_048_576, DEFAULT_MEMTABLE_BUDGET_BYTES); // 1 MiB KV
		assert_eq!(size, DEFAULT_MEMTABLE_BUDGET_BYTES / 1_048_576);
	}

	#[test]
	fn sizing_handles_zero_rows() {
		let size = compute_segment_size(0, 64, DEFAULT_MEMTABLE_BUDGET_BYTES);
		assert_eq!(size, MIN_SEGMENT_ROWS);
	}
}
pub const MIN_SEGMENT_ROWS: usize = 200_000;
const TARGET_MAX_SEGMENTS: u64 = 32;
pub const DEFAULT_MEMTABLE_BUDGET_BYTES: usize = 2 * 1024 * 1024 * 1024; // 2GB

fn compute_segment_size(approx_rows: u64, avg_kv_bytes: usize, mem_budget_bytes: usize) -> usize {
	let avg_kv = avg_kv_bytes.max(1);
	let desired_by_segments = if approx_rows == 0 {
		MIN_SEGMENT_ROWS as u64
	} else {
		let per_seg = (approx_rows + TARGET_MAX_SEGMENTS - 1) / TARGET_MAX_SEGMENTS;
		per_seg.max(MIN_SEGMENT_ROWS as u64)
	};

	let mem_cap_rows = (mem_budget_bytes.max(1) / avg_kv).max(1);
	let chosen = desired_by_segments as usize;
	chosen.min(mem_cap_rows).max(1)
}
