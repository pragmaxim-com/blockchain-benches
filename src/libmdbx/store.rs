use crate::store_interface::{ProgressTracker, StoreRead, StoreWrite};
use libmdbx::{
	Database, DatabaseOptions, Mode, NoWriteMap, ReadWriteOptions, RO, RW, SyncMode, Table, TableFlags, Transaction,
	WriteFlags,
};
use std::{ffi::OsStr, fs, marker::PhantomData, path::{Path, PathBuf}};

pub use crate::store_interface::StoreCodec;

#[derive(Debug)]
pub enum StoreError {
	Mdbx(libmdbx::Error),
	InvalidInput(String),
}

impl std::fmt::Display for StoreError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			StoreError::Mdbx(err) => write!(f, "libmdbx error: {err}"),
			StoreError::InvalidInput(msg) => write!(f, "invalid input: {msg}"),
		}
	}
}

impl std::error::Error for StoreError {}

impl From<libmdbx::Error> for StoreError {
	fn from(err: libmdbx::Error) -> Self {
		StoreError::Mdbx(err)
	}
}

impl From<std::io::Error> for StoreError {
	fn from(err: std::io::Error) -> Self {
		StoreError::InvalidInput(err.to_string())
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
	pub fn plain(from: usize) -> Self {
		Layout::Plain { key_to_value: from }
	}
	pub fn unique_index(from: usize) -> Self {
		Layout::UniqueIndex { key_to_value: from, value_to_key: from + 1 }
	}
	pub fn range(from: usize) -> Self {
		Layout::Range { key_to_value: from, value_key_btree: from + 1 }
	}
	pub fn dictionary(from: usize) -> Self {
		Layout::Dictionary {
			key_to_birth_key: from,
			birth_key_to_value: from + 1,
			value_to_birth_key: from + 2,
			birth_key_key_btree: from + 3,
		}
	}

	fn table_count(&self) -> usize {
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
	db: Database<NoWriteMap>,
	layout: Layout,
	progress: Option<ProgressTracker>,
	_ph: PhantomData<(K, V, KC, VC)>,
}

impl<K, V, KC, VC> Store<K, V, KC, VC>
where
	KC: StoreCodec<K, Error = StoreError>,
	VC: StoreCodec<V, Error = StoreError>,
{
	pub fn open(path: &Path, layout: Layout) -> StoreResult<Self> {
		Self::open_with_options(path, layout, ())
	}

	pub fn open_with_options(path: &Path, layout: Layout, _options: ()) -> StoreResult<Self> {
		let db_path = db_file_path(path)?;
		let rw_opts = ReadWriteOptions { sync_mode: SyncMode::UtterlyNoSync, ..Default::default() };
		let opts = DatabaseOptions { max_tables: Some(layout.table_count() as u64), mode: Mode::ReadWrite(rw_opts), ..Default::default() };
		let db = Database::open_with_options(&db_path, opts)?;
		{
			let tx = db.begin_rw_txn()?;
			for idx in 0..layout.table_count() {
				let name = table_name(idx);
				tx.create_table(Some(&name), TableFlags::empty())?;
			}
			tx.commit()?;
		}
		Ok(Self { db, layout, progress: None, _ph: PhantomData })
	}

	pub fn commit<'a, I>(&mut self, items: I) -> StoreResult<()>
	where
		I: IntoIterator<Item = (&'a K, &'a V)>,
		K: 'a,
		V: 'a,
	{
		let mut processed = 0u64;
		let txn = self.db.begin_rw_txn()?;
		match self.layout {
			Layout::Plain { key_to_value } => {
				let table = open_table(&txn, key_to_value)?;
				for (k, v) in items {
					let kbytes = KC::encode(k);
					let vbytes = VC::encode(v);
					txn.put(&table, kbytes.as_ref(), vbytes.as_ref(), WriteFlags::empty())?;
					processed += 1;
				}
			},
			Layout::UniqueIndex { key_to_value, value_to_key } => {
				let t_k2v = open_table(&txn, key_to_value)?;
				let t_v2k = open_table(&txn, value_to_key)?;
				for (k, v) in items {
					let kbytes = KC::encode(k);
					let vbytes = VC::encode(v);
					txn.put(&t_k2v, kbytes.as_ref(), vbytes.as_ref(), WriteFlags::empty())?;
					txn.put(&t_v2k, vbytes.as_ref(), kbytes.as_ref(), WriteFlags::empty())?;
					processed += 2;
				}
			},
			Layout::Range { key_to_value, value_key_btree } => {
				let t_k2v = open_table(&txn, key_to_value)?;
				let t_vkb = open_table(&txn, value_key_btree)?;
				for (k, v) in items {
					let kbytes = KC::encode(k);
					let vbytes = VC::encode(v);
					txn.put(&t_k2v, kbytes.as_ref(), vbytes.as_ref(), WriteFlags::empty())?;
					let vk = concat(vbytes.as_ref(), kbytes.as_ref());
					txn.put(&t_vkb, vk.as_slice(), &[], WriteFlags::empty())?;
					processed += 2;
				}
			},
			Layout::Dictionary { key_to_birth_key, birth_key_to_value, value_to_birth_key, birth_key_key_btree } => {
				use std::collections::HashMap;
				let t_k2pk = open_table(&txn, key_to_birth_key)?;
				let t_pk2v = open_table(&txn, birth_key_to_value)?;
				let t_v2pk = open_table(&txn, value_to_birth_key)?;
				let t_pk_k = open_table(&txn, birth_key_key_btree)?;
				let mut cache: HashMap<Vec<u8>, (Vec<u8>, bool)> = HashMap::new();
				for (k, v) in items {
					let kbytes = KC::encode(k);
					let vbytes = VC::encode(v);
					let (pk, is_new) = if let Some(entry) = cache.get(vbytes.as_ref()) {
						entry.clone()
					} else if let Some(pk) = txn.get::<Vec<u8>>(&t_v2pk, vbytes.as_ref())? {
						let pk_vec = pk;
						cache.insert(vbytes.as_ref().to_vec(), (pk_vec.clone(), false));
						(pk_vec, false)
					} else {
						let pk_vec = kbytes.as_ref().to_vec();
						cache.insert(vbytes.as_ref().to_vec(), (pk_vec.clone(), true));
						(pk_vec, true)
					};

					if is_new {
						txn.put(&t_v2pk, vbytes.as_ref(), pk.as_slice(), WriteFlags::empty())?;
						txn.put(&t_pk2v, pk.as_slice(), vbytes.as_ref(), WriteFlags::empty())?;
						processed += 2;
					}
					txn.put(&t_k2pk, kbytes.as_ref(), pk.as_slice(), WriteFlags::empty())?;
					let pk_key = concat(&pk, kbytes.as_ref());
					txn.put(&t_pk_k, pk_key.as_slice(), &[], WriteFlags::empty())?;
					processed += 2;
				}
			},
		}
		txn.commit()?;
		if let Some(p) = self.progress.as_mut() {
			p.record(processed);
		}
		Ok(())
	}

	pub fn get_value(&self, key: &K) -> StoreResult<Option<V>> {
		let kbytes = KC::encode(key);
		let txn = self.db.begin_ro_txn()?;
		match self.layout {
			Layout::Plain { key_to_value }
			| Layout::UniqueIndex { key_to_value, .. }
			| Layout::Range { key_to_value, .. } => {
				let table = open_table_ro(&txn, key_to_value)?;
				txn.get::<Vec<u8>>(&table, kbytes.as_ref())?.map(|v| VC::decode(&v)).transpose()
			},
			Layout::Dictionary { key_to_birth_key, birth_key_to_value, .. } => {
				let t_k2pk = open_table_ro(&txn, key_to_birth_key)?;
				let t_pk2v = open_table_ro(&txn, birth_key_to_value)?;
				if let Some(pk) = txn.get::<Vec<u8>>(&t_k2pk, kbytes.as_ref())? {
					txn.get::<Vec<u8>>(&t_pk2v, pk.as_slice())?.map(|v| VC::decode(&v)).transpose()
				} else {
					Ok(None)
				}
			},
		}
	}

	pub fn get_key_for_value(&self, value: &V) -> StoreResult<Option<K>> {
		let vbytes = VC::encode(value);
		let txn = self.db.begin_ro_txn()?;
		match self.layout {
			Layout::UniqueIndex { value_to_key, .. } => {
				let t_v2k = open_table_ro(&txn, value_to_key)?;
				txn.get::<Vec<u8>>(&t_v2k, vbytes.as_ref())?.map(|k| KC::decode(&k)).transpose()
			},
			_ => Err(StoreError::InvalidInput("get_key_for_value not supported for this layout".into())),
		}
	}

	pub fn get_keys_for_value(&self, value: &V) -> StoreResult<Vec<K>> {
		let vbytes = VC::encode(value);
		let txn = self.db.begin_ro_txn()?;
		match self.layout {
			Layout::Range { value_key_btree, .. } => {
				let table = open_table_ro(&txn, value_key_btree)?;
				let mut out = Vec::new();
				let mut iter = txn.cursor(&table)?.into_iter_from::<Vec<u8>, Vec<u8>>(vbytes.as_ref());
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
				let t_v2pk = open_table_ro(&txn, value_to_birth_key)?;
				let t_pk_k = open_table_ro(&txn, birth_key_key_btree)?;
				if let Some(pk) = txn.get::<Vec<u8>>(&t_v2pk, vbytes.as_ref())? {
					let mut out = Vec::new();
					let mut cursor = txn.cursor(&t_pk_k)?;
					let mut iter = cursor.into_iter_from::<Vec<u8>, Vec<u8>>(pk.as_slice());
					while let Some(Ok((k, _))) = iter.next() {
						if !k.starts_with(pk.as_slice()) {
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
		Ok(())
	}
}

fn table_name(idx: usize) -> String {
	format!("col{idx}")
}

fn open_table<'txn>(txn: &'txn Transaction<'txn, RW, NoWriteMap>, idx: usize) -> StoreResult<Table<'txn>> {
	let name = table_name(idx);
	let flags = TableFlags::empty();
	let table = txn.create_table(Some(&name), flags)?;
	Ok(table)
}

fn open_table_ro<'txn>(txn: &'txn Transaction<'txn, RO, NoWriteMap>, idx: usize) -> StoreResult<Table<'txn>> {
	let name = table_name(idx);
	let table = txn.open_table(Some(&name))?;
	Ok(table)
}

fn concat(a: &[u8], b: &[u8]) -> Vec<u8> {
	let mut out = Vec::with_capacity(a.len() + b.len());
	out.extend_from_slice(a);
	out.extend_from_slice(b);
	out
}

fn db_file_path(path: &Path) -> StoreResult<PathBuf> {
	if path.extension() == Some(OsStr::new("mdbx")) {
		return Ok(path.to_path_buf())
	}
	fs::create_dir_all(path)?;
	Ok(path.join("db.mdbx"))
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
			let path = dir.path().join("db.mdbx");
			std::mem::forget(dir);
			Store::<Vec<u8>, Vec<u8>, BytesCodec, BytesCodec>::open_with_options(&path, Layout::plain(0), ()).unwrap()
		});
	}

	#[test]
	fn shared_reverse_suite() {
		reverse_lookup_unique(|| {
			let dir = tempdir().unwrap();
			let path = dir.path().join("db.mdbx");
			std::mem::forget(dir);
			Store::<Vec<u8>, Vec<u8>, BytesCodec, BytesCodec>::open_with_options(&path, Layout::unique_index(0), ()).unwrap()
		});
	}

	#[test]
	fn shared_multiple_keys_suite() {
		multiple_keys_for_value(|| {
			let dir = tempdir().unwrap();
			let path = dir.path().join("db.mdbx");
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
