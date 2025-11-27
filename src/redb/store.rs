use crate::store_interface::{ProgressTracker, StoreRead, StoreWrite};
use redb::{
	CommitError, Database, DatabaseError, Durability, ReadableDatabase, ReadableTable, SetDurabilityError,
	StorageError, TableDefinition, TableError, TransactionError,
};
use std::{ffi::OsStr, fs, marker::PhantomData, path::{Path, PathBuf}};

pub use crate::store_interface::StoreCodec;

#[derive(Debug)]
pub enum StoreError {
	Redb(redb::Error),
	Db(DatabaseError),
	Tx(TransactionError),
	Table(TableError),
	Storage(StorageError),
	SetDurability(SetDurabilityError),
	Commit(CommitError),
	InvalidInput(String),
}

impl std::fmt::Display for StoreError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			StoreError::Redb(err) => write!(f, "redb error: {err}"),
			StoreError::Db(err) => write!(f, "redb db error: {err}"),
			StoreError::Tx(err) => write!(f, "redb tx error: {err}"),
			StoreError::Table(err) => write!(f, "redb table error: {err}"),
			StoreError::Storage(err) => write!(f, "redb storage error: {err}"),
			StoreError::SetDurability(err) => write!(f, "redb durability error: {err}"),
			StoreError::Commit(err) => write!(f, "redb commit error: {err}"),
			StoreError::InvalidInput(msg) => write!(f, "invalid input: {msg}"),
		}
	}
}

impl std::error::Error for StoreError {}

impl From<redb::Error> for StoreError {
	fn from(err: redb::Error) -> Self {
		StoreError::Redb(err)
	}
}

impl From<DatabaseError> for StoreError {
	fn from(err: DatabaseError) -> Self {
		StoreError::Db(err)
	}
}

impl From<TransactionError> for StoreError {
	fn from(err: TransactionError) -> Self {
		StoreError::Tx(err)
	}
}

impl From<StorageError> for StoreError {
	fn from(err: StorageError) -> Self {
		StoreError::Storage(err)
	}
}

impl From<CommitError> for StoreError {
	fn from(err: CommitError) -> Self {
		StoreError::Commit(err)
	}
}

impl From<TableError> for StoreError {
	fn from(err: TableError) -> Self {
		StoreError::Table(err)
	}
}

impl From<SetDurabilityError> for StoreError {
	fn from(err: SetDurabilityError) -> Self {
		StoreError::SetDurability(err)
	}
}

impl From<std::io::Error> for StoreError {
	fn from(err: std::io::Error) -> Self {
		StoreError::Storage(StorageError::Io(err))
	}
}

impl StoreError {
	fn other<E: std::fmt::Display>(err: E) -> Self {
		StoreError::InvalidInput(err.to_string())
	}
}

pub type StoreResult<T> = Result<T, StoreError>;

/// Storage layouts supported by the generic store.
#[derive(Clone, Copy)]
pub enum Layout {
	Plain,
	UniqueIndex,
	Range,
	Dictionary,
}

impl Layout {
	pub fn plain() -> Self {
		Layout::Plain
	}
	pub fn unique_index() -> Self {
		Layout::UniqueIndex
	}
	pub fn range() -> Self {
		Layout::Range
	}
	pub fn dictionary() -> Self {
		Layout::Dictionary
	}
}

/// Generic store operating on a chosen layout and codecs.
pub struct Store<K, V, KC, VC>
where
	KC: StoreCodec<K, Error = StoreError>,
	VC: StoreCodec<V, Error = StoreError>,
{
	db: Database,
	layout: Layout,
	progress: Option<ProgressTracker>,
	_ph: PhantomData<(K, V, KC, VC)>,
}

const KEY_TO_VALUE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("k2v");
const VALUE_TO_KEY: TableDefinition<&[u8], &[u8]> = TableDefinition::new("v2k");
const VALUE_KEY_BTREE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("vkb");
const KEY_TO_BIRTH_KEY: TableDefinition<&[u8], &[u8]> = TableDefinition::new("k2pk");
const BIRTH_KEY_TO_VALUE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("pk2v");
const VALUE_TO_BIRTH_KEY: TableDefinition<&[u8], &[u8]> = TableDefinition::new("v2pk");
const BIRTH_KEY_KEY_BTREE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("pkkb");

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
		let db = Database::create(db_path)?;
		{
			let tx = db.begin_write()?;
			match layout {
				Layout::Plain => {
					tx.open_table(KEY_TO_VALUE)?;
				},
				Layout::UniqueIndex => {
					tx.open_table(KEY_TO_VALUE)?;
					tx.open_table(VALUE_TO_KEY)?;
				},
				Layout::Range => {
					tx.open_table(KEY_TO_VALUE)?;
					tx.open_table(VALUE_KEY_BTREE)?;
				},
				Layout::Dictionary => {
					tx.open_table(KEY_TO_BIRTH_KEY)?;
					tx.open_table(BIRTH_KEY_TO_VALUE)?;
					tx.open_table(VALUE_TO_BIRTH_KEY)?;
					tx.open_table(BIRTH_KEY_KEY_BTREE)?;
				},
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
		let mut write_tx = self.db.begin_write()?;
		match self.layout {
			Layout::Plain => {
				let mut k2v = write_tx.open_table(KEY_TO_VALUE)?;
				for (k, v) in items {
					let kbytes = KC::encode(k);
					let vbytes = VC::encode(v);
					k2v.insert(kbytes.as_ref(), vbytes.as_ref())?;
					processed += 1;
				}
			},
			Layout::UniqueIndex => {
				let mut k2v = write_tx.open_table(KEY_TO_VALUE)?;
				let mut v2k = write_tx.open_table(VALUE_TO_KEY)?;
				for (k, v) in items {
					let kbytes = KC::encode(k);
					let vbytes = VC::encode(v);
					k2v.insert(kbytes.as_ref(), vbytes.as_ref())?;
					v2k.insert(vbytes.as_ref(), kbytes.as_ref())?;
					processed += 2;
				}
			},
			Layout::Range => {
				let mut k2v = write_tx.open_table(KEY_TO_VALUE)?;
				let mut vkb = write_tx.open_table(VALUE_KEY_BTREE)?;
				for (k, v) in items {
					let kbytes = KC::encode(k);
					let vbytes = VC::encode(v);
					k2v.insert(kbytes.as_ref(), vbytes.as_ref())?;
					let vk = concat(vbytes.as_ref(), kbytes.as_ref());
					vkb.insert(vk.as_slice(), &[] as &[u8])?;
					processed += 2;
				}
			},
			Layout::Dictionary => {
				use std::collections::HashMap;
				let mut k2pk = write_tx.open_table(KEY_TO_BIRTH_KEY)?;
				let mut pk2v = write_tx.open_table(BIRTH_KEY_TO_VALUE)?;
				let mut v2pk = write_tx.open_table(VALUE_TO_BIRTH_KEY)?;
				let mut pk_k_btree = write_tx.open_table(BIRTH_KEY_KEY_BTREE)?;
				let mut cache: HashMap<Vec<u8>, (Vec<u8>, bool)> = HashMap::new();
				for (k, v) in items {
					let kbytes = KC::encode(k);
					let vbytes = VC::encode(v);
					let (pk, is_new) = if let Some(entry) = cache.get(vbytes.as_ref()) {
						entry.clone()
					} else if let Ok(Some(pk)) = v2pk.get(vbytes.as_ref()) {
						let pk_vec = pk.value().to_vec();
						cache.insert(vbytes.as_ref().to_vec(), (pk_vec.clone(), false));
						(pk_vec, false)
					} else {
						let pk_vec = kbytes.as_ref().to_vec();
						cache.insert(vbytes.as_ref().to_vec(), (pk_vec.clone(), true));
						(pk_vec, true)
					};

					if is_new {
						v2pk.insert(vbytes.as_ref(), pk.as_slice())?;
						pk2v.insert(pk.as_slice(), vbytes.as_ref())?;
						processed += 2;
					}
					k2pk.insert(kbytes.as_ref(), pk.as_slice())?;
					let pk_key = concat(&pk, kbytes.as_ref());
					pk_k_btree.insert(pk_key.as_slice(), &[] as &[u8])?;
					processed += 2;
				}
			},
		}
		write_tx.set_durability(Durability::None)?;
		write_tx.commit()?;
		if let Some(p) = self.progress.as_mut() {
			p.record(processed);
		}
		Ok(())
	}

	pub fn get_value(&self, key: &K) -> StoreResult<Option<V>> {
		let kbytes = KC::encode(key);
		let read_tx = self.db.begin_read().map_err(StoreError::other)?;
		match self.layout {
			Layout::Plain | Layout::UniqueIndex | Layout::Range => {
				let k2v = read_tx.open_table(KEY_TO_VALUE).map_err(StoreError::other)?;
				k2v.get(kbytes.as_ref())?
					.map(|v| VC::decode(v.value()))
					.transpose()
			},
			Layout::Dictionary => {
				let k2pk = read_tx.open_table(KEY_TO_BIRTH_KEY).map_err(StoreError::other)?;
				let pk2v = read_tx.open_table(BIRTH_KEY_TO_VALUE).map_err(StoreError::other)?;
				if let Some(pk) = k2pk.get(kbytes.as_ref())? {
					pk2v.get(pk.value())?
						.map(|v| VC::decode(v.value()))
						.transpose()
				} else {
					Ok(None)
				}
			},
		}
	}

	pub fn get_key_for_value(&self, value: &V) -> StoreResult<Option<K>> {
		let vbytes = VC::encode(value);
		let read_tx = self.db.begin_read().map_err(StoreError::other)?;
		match self.layout {
			Layout::UniqueIndex => {
				let v2k = read_tx.open_table(VALUE_TO_KEY).map_err(StoreError::other)?;
				v2k.get(vbytes.as_ref())?.map(|k| KC::decode(k.value())).transpose()
			},
			_ => Err(StoreError::InvalidInput("get_key_for_value not supported for this layout".into())),
		}
	}

	pub fn get_keys_for_value(&self, value: &V) -> StoreResult<Vec<K>> {
		let vbytes = VC::encode(value);
		let read_tx = self.db.begin_read().map_err(StoreError::other)?;
		match self.layout {
			Layout::Range => {
				let vkb = read_tx.open_table(VALUE_KEY_BTREE).map_err(StoreError::other)?;
				let mut out = Vec::new();
				let mut cursor = vkb.range(vbytes.as_ref()..)?;
				while let Some(Ok((k, _))) = cursor.next() {
					let kslice = k.value();
					if !kslice.starts_with(vbytes.as_ref()) {
						break
					}
					let key_bytes = &kslice[vbytes.as_ref().len()..];
					out.push(KC::decode(key_bytes)?);
				}
				Ok(out)
			},
			Layout::Dictionary => {
				let v2pk = read_tx.open_table(VALUE_TO_BIRTH_KEY).map_err(StoreError::other)?;
				let pk_k_btree = read_tx.open_table(BIRTH_KEY_KEY_BTREE).map_err(StoreError::other)?;
				if let Some(pk) = v2pk.get(vbytes.as_ref())? {
					let pk = pk.value();
					let mut out = Vec::new();
					let mut cursor = pk_k_btree.range(pk..)?;
					while let Some(Ok((k, _))) = cursor.next() {
						let kslice = k.value();
						if !kslice.starts_with(pk) {
							break
						}
						let key_bytes = &kslice[pk.len()..];
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

fn db_file_path(path: &Path) -> StoreResult<PathBuf> {
	if path.extension() == Some(OsStr::new("redb")) {
		return Ok(path.to_path_buf())
	}
	fs::create_dir_all(path)?;
	Ok(path.join("db.redb"))
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
			let path = dir.path().join("db.redb");
			std::mem::forget(dir);
			Store::<Vec<u8>, Vec<u8>, BytesCodec, BytesCodec>::open_with_options(&path, Layout::plain(), ()).unwrap()
		});
	}

	#[test]
	fn shared_reverse_suite() {
		reverse_lookup_unique(|| {
			let dir = tempdir().unwrap();
			let path = dir.path().join("db.redb");
			std::mem::forget(dir);
			Store::<Vec<u8>, Vec<u8>, BytesCodec, BytesCodec>::open_with_options(&path, Layout::unique_index(), ()).unwrap()
		});
	}

	#[test]
	fn shared_multiple_keys_suite() {
		multiple_keys_for_value(|| {
			let dir = tempdir().unwrap();
			let path = dir.path().join("db.redb");
			std::mem::forget(dir);
			Store::<Vec<u8>, Vec<u8>, BytesCodec, BytesCodec>::open_with_options(&path, Layout::range(), ()).unwrap()
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
