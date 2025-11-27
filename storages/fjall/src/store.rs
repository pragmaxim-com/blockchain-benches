use core::store_interface::{ProgressTracker, StoreCodec, StoreRead, StoreWrite};
use fjall::{Config, Keyspace, Partition, PartitionCreateOptions, PersistMode};
use std::{marker::PhantomData, path::Path};

#[derive(Debug)]
pub enum StoreError {
	Fjall(fjall::Error),
	InvalidInput(String),
}

impl std::fmt::Display for StoreError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			StoreError::Fjall(err) => write!(f, "fjall error: {err}"),
			StoreError::InvalidInput(msg) => write!(f, "invalid input: {msg}"),
		}
	}
}

impl std::error::Error for StoreError {}

impl From<fjall::Error> for StoreError {
	fn from(err: fjall::Error) -> Self {
		StoreError::Fjall(err)
	}
}

pub type StoreResult<T> = Result<T, StoreError>;

#[derive(Clone, Copy)]
pub struct FjallOptions {
	pub max_journal_bytes: u64,
	pub max_write_buffer_bytes: u64,
	pub cache_bytes: u64,
	pub flush_workers: usize,
	pub compaction_workers: usize,
	pub manual_journal_persist: bool,
}

impl Default for FjallOptions {
	fn default() -> Self {
		let cpus = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
		Self {
			max_journal_bytes: 1 * 1024 * 1024 * 1024,     // 4 GiB WAL budget
			max_write_buffer_bytes: 128 * 1024 * 1024,     // 512 MiB memtables across partitions
			cache_bytes: 512 * 1024 * 1024,                // 512 MiB cache
			flush_workers: cpus.max(4),
			compaction_workers: cpus.max(4),
			manual_journal_persist: true,                  // favor write throughput over durability
		}
	}
}

/// Storage layouts supported by the generic store.
#[derive(Clone, Copy)]
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
			Layout::Plain { key_to_value } => (*key_to_value as usize) + 1,
			Layout::UniqueIndex { value_to_key, .. } => (*value_to_key as usize) + 1,
			Layout::Range { value_key_btree, .. } => (*value_key_btree as usize) + 1,
			Layout::Dictionary { birth_key_key_btree, .. } => (*birth_key_key_btree as usize) + 1,
		}
	}
}

/// Generic store operating on a chosen layout and codecs.
pub struct Store<K, V, KC, VC>
where
	KC: StoreCodec<K, Error = StoreError>,
	VC: StoreCodec<V, Error = StoreError>,
{
	keyspace: Keyspace,
	layout: Layout,
	partitions: Vec<Partition>,
	progress: Option<ProgressTracker>,
	_ph: PhantomData<(K, V, KC, VC)>,
}

impl<K, V, KC, VC> Store<K, V, KC, VC>
where
	KC: StoreCodec<K, Error = StoreError>,
	VC: StoreCodec<V, Error = StoreError>,
{
	pub fn open(path: &Path, layout: Layout) -> StoreResult<Self> {
		Self::open_with_options(path, layout, FjallOptions::default())
	}

	pub fn open_with_options(path: &Path, layout: Layout, options: FjallOptions) -> StoreResult<Self> {
		let keyspace = Config::new(path)
			.manual_journal_persist(options.manual_journal_persist)
			.max_journaling_size(options.max_journal_bytes)
			.max_write_buffer_size(options.max_write_buffer_bytes)
			.cache_size(options.cache_bytes)
			.flush_workers(options.flush_workers)
			.compaction_workers(options.compaction_workers)
			.open()?;
		let count = layout.column_count();
		let mut partitions = Vec::with_capacity(count);
		for idx in 0..count {
			let name = format!("col{idx}");
			partitions.push(keyspace.open_partition(&name, PartitionCreateOptions::default())?);
		}
		Ok(Self { keyspace, layout, partitions, progress: None, _ph: PhantomData })
	}

	pub fn commit<'a, I>(&mut self, items: I) -> StoreResult<()>
	where
		I: IntoIterator<Item = (&'a K, &'a V)>,
		K: 'a,
		V: 'a,
	{
		let mut processed = 0u64;
		match self.layout {
			Layout::Plain { key_to_value } => {
				let ks = &self.partitions[key_to_value as usize];
				for (k, v) in items {
					let kbytes = KC::encode(k);
					let vbytes = VC::encode(v);
					ks.insert(kbytes.as_ref(), vbytes.as_ref())?;
					processed += 1;
				}
			},
			Layout::UniqueIndex { key_to_value, value_to_key } => {
				let ksv = &self.partitions[key_to_value as usize];
				let ksk = &self.partitions[value_to_key as usize];
				for (k, v) in items {
					let kbytes = KC::encode(k);
					let vbytes = VC::encode(v);
					ksv.insert(kbytes.as_ref(), vbytes.as_ref())?;
					ksk.insert(vbytes.as_ref(), kbytes.as_ref())?;
					processed += 2;
				}
			},
			Layout::Range { key_to_value, value_key_btree } => {
				let kv_ks = &self.partitions[key_to_value as usize];
				let btree_ks = &self.partitions[value_key_btree as usize];
				for (k, v) in items {
					let kbytes = KC::encode(k);
					let vbytes = VC::encode(v);
					let kslice = kbytes.as_ref();
					kv_ks.insert(kslice, vbytes.as_ref())?;
					let vk = concat(vbytes.as_ref(), kslice);
					btree_ks.insert(&vk, &[])?;
					processed += 2;
				}
			},
			Layout::Dictionary { key_to_birth_key, birth_key_to_value, value_to_birth_key, birth_key_key_btree } => {
				use std::collections::HashMap;
				let k2pk = &self.partitions[key_to_birth_key as usize];
				let pk2v = &self.partitions[birth_key_to_value as usize];
				let v2pk = &self.partitions[value_to_birth_key as usize];
				let pk_k_btree = &self.partitions[birth_key_key_btree as usize];
				let mut value_cache: HashMap<Vec<u8>, (Vec<u8>, bool)> = HashMap::new();
				for (k, v) in items {
					let kbytes = KC::encode(k);
					let vbytes = VC::encode(v);
					let (pk, is_new) = if let Some(entry) = value_cache.get(vbytes.as_ref()) {
						entry.clone()
					} else if let Some(pk) = v2pk.get(vbytes.as_ref())? {
						let pk_vec = pk.as_ref().to_vec();
						value_cache.insert(vbytes.as_ref().to_vec(), (pk_vec.clone(), false));
						(pk_vec, false)
					} else {
						let pk_vec = kbytes.as_ref().to_vec();
						value_cache.insert(vbytes.as_ref().to_vec(), (pk_vec.clone(), true));
						(pk_vec, true)
					};

					if is_new {
						v2pk.insert(vbytes.as_ref(), &pk)?;
						pk2v.insert(&pk, vbytes.as_ref())?;
						processed += 2;
					}
					k2pk.insert(kbytes.as_ref(), &pk)?;
					let pk_key = concat(&pk, kbytes.as_ref());
					pk_k_btree.insert(&pk_key, &[])?;
					processed += 2;
				}
			},
		}
		if let Some(p) = self.progress.as_mut() {
			p.record(processed);
		}
		Ok(())
	}

	pub fn get_value(&self, key: &K) -> StoreResult<Option<V>> {
		let kbytes = KC::encode(key);
		match self.layout {
			Layout::Plain { key_to_value }
			| Layout::UniqueIndex { key_to_value, .. }
			| Layout::Range { key_to_value, .. } => {
				self.partitions[key_to_value as usize]
					.get(kbytes.as_ref())?
					.map(|b| VC::decode(b.as_ref()))
					.transpose()
			},
			Layout::Dictionary { key_to_birth_key, birth_key_to_value, .. } => {
				if let Some(pk) = self.partitions[key_to_birth_key as usize].get(kbytes.as_ref())? {
					self.partitions[birth_key_to_value as usize]
						.get(pk.as_ref())?
						.map(|b| VC::decode(b.as_ref()))
						.transpose()
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
				self.partitions[value_to_key as usize]
					.get(vbytes.as_ref())?
					.map(|b| KC::decode(b.as_ref()))
					.transpose()
			},
			_ => Err(StoreError::InvalidInput("get_key_for_value not supported for this layout".into())),
		}
	}

	pub fn get_keys_for_value(&self, value: &V) -> StoreResult<Vec<K>> {
		let vbytes = VC::encode(value);
		match self.layout {
			Layout::Range { value_key_btree, .. } => {
				let prefix = vbytes.as_ref().to_vec();
				let mut out = Vec::new();
				for kv in self.partitions[value_key_btree as usize].prefix(&prefix) {
					let (k, _) = kv?;
					let key_bytes = &k.as_ref()[prefix.len()..];
					out.push(KC::decode(key_bytes)?);
				}
				Ok(out)
			},
			Layout::Dictionary { value_to_birth_key, birth_key_key_btree, .. } => {
				if let Some(pk) = self.partitions[value_to_birth_key as usize].get(vbytes.as_ref())? {
					let prefix = pk.as_ref().to_vec();
					let mut iter = self.partitions[birth_key_key_btree as usize].prefix(&prefix);
					let mut out = Vec::new();
					while let Some(kv) = iter.next() {
						let (k, _) = kv?;
						if k.len() < prefix.len() || k.as_ref()[..prefix.len()] != prefix[..] {
							break
						}
						let key_bytes = &k.as_ref()[prefix.len()..];
						out.push(KC::decode(key_bytes)?);
					}
					Ok(out)
				} else {
					Ok(Vec::new())
				}
			},
			_ => Err(StoreError::InvalidInput("get_keys_for_value not supported for this layout".into())),
		}
	}

	pub fn flush(&mut self) -> StoreResult<()> {
		self.keyspace.persist(PersistMode::SyncData)?;
		Ok(())
	}
}

fn concat(a: &[u8], b: &[u8]) -> Vec<u8> {
	let mut out = Vec::with_capacity(a.len() + b.len());
	out.extend_from_slice(a);
	out.extend_from_slice(b);
	out
}

#[cfg(test)]
mod tests {
	use super::*;
	use core::store_tests::{basic_value_roundtrip, multiple_keys_for_value, reverse_lookup_unique};
	use tempfile::tempdir;

	struct BytesCodec;

	impl StoreCodec<Vec<u8>> for BytesCodec {
		type Error = StoreError;
		type Enc<'a> = &'a [u8] where Self: 'a, Vec<u8>: 'a;
		fn encode<'a>(value: &'a Vec<u8>) -> Self::Enc<'a> {
			value.as_slice()
		}
		fn decode(bytes: &[u8]) -> StoreResult<Vec<u8>> {
			Ok(bytes.to_vec())
		}
	}

	#[test]
	fn shared_basic_suite() {
		basic_value_roundtrip(|| {
			let dir = tempdir().unwrap();
			let path = dir.path().to_path_buf();
			std::mem::forget(dir);
			Store::<Vec<u8>, Vec<u8>, BytesCodec, BytesCodec>::open_with_options(
				&path,
				Layout::plain(0),
				FjallOptions::default(),
			)
			.unwrap()
		});
	}

	#[test]
	fn shared_reverse_suite() {
		reverse_lookup_unique(|| {
			let dir = tempdir().unwrap();
			let path = dir.path().to_path_buf();
			std::mem::forget(dir);
			Store::<Vec<u8>, Vec<u8>, BytesCodec, BytesCodec>::open_with_options(
				&path,
				Layout::unique_index(0),
				FjallOptions::default(),
			)
			.unwrap()
		});
	}

	#[test]
	fn shared_multiple_keys_suite() {
		multiple_keys_for_value(|| {
			let dir = tempdir().unwrap();
			let path = dir.path().to_path_buf();
			std::mem::forget(dir);
			Store::<Vec<u8>, Vec<u8>, BytesCodec, BytesCodec>::open_with_options(
				&path,
				Layout::range(0),
				FjallOptions::default(),
			)
			.unwrap()
		});
	}
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
	type Options = FjallOptions;
	type Layout = Layout;

	fn open_with_options(path: &Path, layout: Self::Layout, options: Self::Options) -> StoreResult<Self> {
		Store::open_with_options(path, layout, options)
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
