use crate::store_interface::{ProgressTracker, StoreRead, StoreWrite};
use rocksdb::{Direction, IteratorMode, Options, WriteBatch, WriteOptions, DBWithThreadMode, MultiThreaded};
use std::{collections::HashMap, marker::PhantomData, path::Path, sync::Arc};

pub use crate::store_interface::StoreCodec;

#[derive(Debug)]
pub enum StoreError {
	Rocks(rocksdb::Error),
	InvalidInput(String),
}

impl std::fmt::Display for StoreError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			StoreError::Rocks(err) => write!(f, "rocksdb error: {err}"),
			StoreError::InvalidInput(msg) => write!(f, "invalid input: {msg}"),
		}
	}
}

impl std::error::Error for StoreError {}

impl From<rocksdb::Error> for StoreError {
	fn from(err: rocksdb::Error) -> Self {
		StoreError::Rocks(err)
	}
}

pub type StoreResult<T> = Result<T, StoreError>;

#[derive(Clone, Copy)]
pub enum Layout {
	Plain { key_to_value: usize },
	UniqueIndex { key_to_value: usize, value_to_key: usize },
	Range { key_to_value: usize, value_key_btree: usize },
	Dictionary { key_to_birth_key: usize, birth_key_to_value: usize, value_to_birth_key: usize, birth_key_key_btree: usize },
}

impl Layout {
	pub fn plain(from: usize) -> Layout {
		Layout::Plain { key_to_value: from }
	}
	pub fn unique_index(from: usize) -> Layout {
		Layout::UniqueIndex { key_to_value: from, value_to_key: from + 1 }
	}
	pub fn range(from: usize) -> Layout {
		Layout::Range { key_to_value: from, value_key_btree: from + 1 }
	}
	pub fn dictionary(from: usize) -> Layout {
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

pub struct Store<K, V, KC, VC>
where
	KC: StoreCodec<K, Error = StoreError>,
	VC: StoreCodec<V, Error = StoreError>,
{
	db: DBWithThreadMode<MultiThreaded>,
	cf_names: Vec<String>,
	layout: Layout,
	progress: Option<ProgressTracker>,
	_ph: PhantomData<(K, V, KC, VC)>,
}

impl<K, V, KC, VC> Store<K, V, KC, VC>
where
	KC: StoreCodec<K, Error = StoreError>,
	VC: StoreCodec<V, Error = StoreError>,
{
	fn cf(&self, idx: usize) -> StoreResult<Arc<rocksdb::BoundColumnFamily<'_>>> {
		let name = &self.cf_names[idx];
		self.db
			.cf_handle(name)
			.map(|cf| cf.clone())
			.ok_or_else(|| StoreError::InvalidInput(format!("missing column family {name}")))
	}

	pub fn open(path: &Path, layout: Layout) -> StoreResult<Self> {
		Self::open_with_options(path, layout, ())
	}

	pub fn open_with_options(path: &Path, layout: Layout, _options: ()) -> StoreResult<Self> {
		let mut opts = Options::default();
		opts.create_if_missing(true);
		opts.create_missing_column_families(true);
		let cf_names: Vec<String> = (0..layout.column_count()).map(|i| format!("col{i}")).collect();
		let cf_strs: Vec<&str> = cf_names.iter().map(|s| s.as_str()).collect();
		let db = DBWithThreadMode::<MultiThreaded>::open_cf(&opts, path, cf_strs.clone())?;
		Ok(Self { db, cf_names: cf_names.into_iter().collect(), layout, progress: None, _ph: PhantomData })
	}

	pub fn commit<'a, I>(&mut self, items: I) -> StoreResult<()>
	where
		I: IntoIterator<Item = (&'a K, &'a V)>,
		K: 'a,
		V: 'a,
	{
		let mut processed = 0u64;
		let mut batch = WriteBatch::default();
		let opts = WriteOptions::default();
		match self.layout {
			Layout::Plain { key_to_value } => {
				let cf = self.cf(key_to_value)?;
				for (k, v) in items {
					let kbytes = KC::encode(k);
					let vbytes = VC::encode(v);
					batch.put_cf(&cf, kbytes.as_ref(), vbytes.as_ref());
					processed += 1;
				}
			},
			Layout::UniqueIndex { key_to_value, value_to_key } => {
				let cf_k2v = self.cf(key_to_value)?;
				let cf_v2k = self.cf(value_to_key)?;
				for (k, v) in items {
					let kbytes = KC::encode(k);
					let vbytes = VC::encode(v);
					batch.put_cf(&cf_k2v, kbytes.as_ref(), vbytes.as_ref());
					batch.put_cf(&cf_v2k, vbytes.as_ref(), kbytes.as_ref());
					processed += 2;
				}
			},
			Layout::Range { key_to_value, value_key_btree } => {
				let cf_k2v = self.cf(key_to_value)?;
				let cf_vkb = self.cf(value_key_btree)?;
				for (k, v) in items {
					let kbytes = KC::encode(k);
					let vbytes = VC::encode(v);
					let kslice = kbytes.as_ref();
					batch.put_cf(&cf_k2v, kslice, vbytes.as_ref());
					let vk = concat(vbytes.as_ref(), kslice);
					batch.put_cf(&cf_vkb, vk.as_slice(), &[]);
					processed += 2;
				}
			},
			Layout::Dictionary { key_to_birth_key, birth_key_to_value, value_to_birth_key, birth_key_key_btree } => {
				use std::collections::HashMap;
				let cf_k2pk = self.cf(key_to_birth_key)?;
				let cf_pk2v = self.cf(birth_key_to_value)?;
				let cf_v2pk = self.cf(value_to_birth_key)?;
				let cf_pk_k = self.cf(birth_key_key_btree)?;
				let mut cache: HashMap<Vec<u8>, (Vec<u8>, bool)> = HashMap::new();
				for (k, v) in items {
					let kbytes = KC::encode(k);
					let vbytes = VC::encode(v);
					let (pk, is_new) = if let Some(entry) = cache.get(vbytes.as_ref()) {
						entry.clone()
					} else if let Some(pk) = self.db.get_cf(&cf_v2pk, vbytes.as_ref())? {
						let pk_vec = pk.to_vec();
						cache.insert(vbytes.as_ref().to_vec(), (pk_vec.clone(), false));
						(pk_vec, false)
					} else {
						let pk_vec = kbytes.as_ref().to_vec();
						cache.insert(vbytes.as_ref().to_vec(), (pk_vec.clone(), true));
						(pk_vec, true)
					};

					if is_new {
						batch.put_cf(&cf_v2pk, vbytes.as_ref(), pk.as_slice());
						batch.put_cf(&cf_pk2v, pk.as_slice(), vbytes.as_ref());
						processed += 2;
					}
					batch.put_cf(&cf_k2pk, kbytes.as_ref(), pk.as_slice());
					let pk_key = concat(&pk, kbytes.as_ref());
					batch.put_cf(&cf_pk_k, pk_key.as_slice(), &[]);
					processed += 2;
				}
			},
		}
		self.db.write_opt(batch, &opts)?;
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
				let cf = self.cf(key_to_value)?;
				self.db.get_cf(&cf, kbytes.as_ref())?.map(|v| VC::decode(&v)).transpose()
			},
			Layout::Dictionary { key_to_birth_key, birth_key_to_value, .. } => {
				let cf_k2pk = self.cf(key_to_birth_key)?;
				let cf_pk2v = self.cf(birth_key_to_value)?;
				if let Some(pk) = self.db.get_cf(&cf_k2pk, kbytes.as_ref())? {
					self.db.get_cf(&cf_pk2v, &pk)?.map(|v| VC::decode(&v)).transpose()
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
				let cf = self.cf(value_to_key)?;
				self.db.get_cf(&cf, vbytes.as_ref())?.map(|k| KC::decode(&k)).transpose()
			},
			_ => Err(StoreError::InvalidInput("get_key_for_value not supported for this layout".into())),
		}
	}

	pub fn get_keys_for_value(&self, value: &V) -> StoreResult<Vec<K>> {
		let vbytes = VC::encode(value);
		match self.layout {
			Layout::Range { value_key_btree, .. } => {
				let cf = self.cf(value_key_btree)?;
				let mut out = Vec::new();
				let mut iter = self
					.db
					.iterator_cf(&cf, IteratorMode::From(vbytes.as_ref(), Direction::Forward));
				while let Some(Ok((k, _))) = iter.next() {
					if !k.starts_with(vbytes.as_ref()) {
						break
					}
					let key_bytes = &k[vbytes.as_ref().len()..];
					out.push(KC::decode(key_bytes)?);
				}
				Ok(out)
			},
			Layout::Dictionary { value_to_birth_key, birth_key_key_btree, .. } => {
				let cf_v2pk = self.cf(value_to_birth_key)?;
				let cf_pk_k = self.cf(birth_key_key_btree)?;
				if let Some(pk) = self.db.get_cf(&cf_v2pk, vbytes.as_ref())? {
					let mut out = Vec::new();
					let mut iter = self.db.iterator_cf(&cf_pk_k, IteratorMode::From(pk.as_ref(), Direction::Forward));
					while let Some(Ok((k, _))) = iter.next() {
						if !k.starts_with(pk.as_ref()) {
							break
						}
						let key_bytes = &k[pk.len()..];
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
		self.db.flush()?;
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
	use crate::store_tests::{basic_value_roundtrip, multiple_keys_for_value, reverse_lookup_unique};
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
			Store::<Vec<u8>, Vec<u8>, BytesCodec, BytesCodec>::open_with_options(&path, Layout::plain(0), ()).unwrap()
		});
	}

	#[test]
	fn shared_reverse_suite() {
		reverse_lookup_unique(|| {
			let dir = tempdir().unwrap();
			let path = dir.path().to_path_buf();
			std::mem::forget(dir);
			Store::<Vec<u8>, Vec<u8>, BytesCodec, BytesCodec>::open_with_options(&path, Layout::unique_index(0), ())
				.unwrap()
		});
	}

	#[test]
	fn shared_multiple_keys_suite() {
		multiple_keys_for_value(|| {
			let dir = tempdir().unwrap();
			let path = dir.path().to_path_buf();
			std::mem::forget(dir);
			Store::<Vec<u8>, Vec<u8>, BytesCodec, BytesCodec>::open_with_options(&path, Layout::range(0), ()).unwrap()
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
	type Options = ();
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
