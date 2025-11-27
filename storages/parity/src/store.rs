use parity_db::{ColId, CompressionType, Db, Error, Options, Result};
use std::{marker::PhantomData, path::Path};
use core::store_interface::{ProgressTracker, StoreCodec, StoreRead, StoreWrite};

pub type StoreResult<T> = Result<T>;

/// Storage layouts supported by the generic store.
#[derive(Clone, Copy)]
pub enum Layout {
	Plain { key_to_value: u8 },
	UniqueIndex { key_to_value: u8, value_to_key: u8 },
	Range { key_to_value: u8, value_key_btree: u8 },
	Dictionary { key_to_birth_key: u8, birth_key_to_value: u8, value_to_birth_key: u8, birth_key_key_btree: u8 },
}

impl Layout {
	pub fn plain(from: ColId) -> Layout {
		Layout::Plain { key_to_value: from }
	}
	pub fn unique_index(from: ColId) -> Layout {
		Layout::UniqueIndex { key_to_value: from, value_to_key: from + 1 }
	}
	pub fn range(from: ColId) -> Layout {
		Layout::Range { key_to_value: from, value_key_btree: from + 1 }
	}
	pub fn dictionary(from: ColId) -> Layout {
		Layout::Dictionary {
			key_to_birth_key: from,
			birth_key_to_value: from + 1,
			value_to_birth_key: from + 2,
			birth_key_key_btree: from + 3,
		}
	}
}

/// Generic store operating on a chosen layout and codecs.
pub struct Store<K, V, KC, VC>
where
	KC: StoreCodec<K, Error = Error>,
	VC: StoreCodec<V, Error = Error>,
{
	db: Db,
	layout: Layout,
    progress: Option<ProgressTracker>,
	_ph: PhantomData<(K, V, KC, VC)>,
}

impl<K, V, KC, VC> Store<K, V, KC, VC>
where
    KC: StoreCodec<K, Error = Error>,
    VC: StoreCodec<V, Error = Error>,
{
    pub fn open(path: &Path, layout: Layout) -> Result<Self> {
        Self::open_with_options(path, layout, ())
    }

	pub fn open_with_options(path: &Path, layout: Layout, _options: ()) -> Result<Self> {
		let options = build_options(path, &layout);
		let db = Db::open_or_create(&options)?;
		Ok(Self { db, progress: None, layout, _ph: PhantomData })
	}

	pub fn commit<'a, I>(&mut self, items: I) -> Result<()>
	where I: IntoIterator<Item = (&'a K, &'a V)>, K: 'a, V: 'a,
	{
        let mut processed = 0u64;
		match self.layout {
			Layout::Plain { key_to_value } => {
				let changes = items
					.into_iter()
					.map(|(k, v)| {
						let kbytes = KC::encode(k);
						let vbytes = VC::encode(v);
						(key_to_value, kbytes.as_ref().to_vec(), Some(vbytes.as_ref().to_vec()))
					})
					.collect::<Vec<_>>();
                processed += changes.len() as u64;
				self.db.commit(changes)?
			},
			Layout::UniqueIndex { key_to_value, value_to_key } => {
				let mut changes = Vec::new();
				for (k, v) in items {
					let kbytes = KC::encode(k);
					let vbytes = VC::encode(v);
					changes.push((key_to_value, kbytes.as_ref().to_vec(), Some(vbytes.as_ref().to_vec())));
					changes.push((value_to_key, vbytes.as_ref().to_vec(), Some(kbytes.as_ref().to_vec())));
				}
                processed += changes.len() as u64;
				self.db.commit(changes)?
			},
			Layout::Range { key_to_value, value_key_btree } => {
				let mut changes = Vec::new();
				for (k, v) in items {
					let kbytes = KC::encode(k);
					let vbytes = VC::encode(v);
					let kslice = kbytes.as_ref();
					changes.push((key_to_value, kslice.to_vec(), Some(vbytes.as_ref().to_vec())));
					let vk = concat(vbytes.as_ref(), kslice);
					changes.push((value_key_btree, vk, Some(Vec::new())));
				}
                processed += changes.len() as u64;
				self.db.commit(changes)?
			},
			Layout::Dictionary { key_to_birth_key, birth_key_to_value, value_to_birth_key, birth_key_key_btree } => {
				use std::collections::HashMap;
				let mut changes = Vec::new();
				// Cache: value bytes -> (birth_key bytes, is_new_birth)
				let mut value_cache: HashMap<Vec<u8>, (Vec<u8>, bool)> = HashMap::new();
				for (k, v) in items {
					let kbytes = KC::encode(k);
					let vbytes = VC::encode(v);
					let (pk, is_new) = if let Some(entry) = value_cache.get(vbytes.as_ref()) {
						entry.clone()
					} else if let Some(pk) = self.db.get(value_to_birth_key, vbytes.as_ref())? {
						value_cache.insert(vbytes.as_ref().to_vec(), (pk.clone(), false));
						(pk, false)
					} else {
						let pk_bytes = kbytes.as_ref().to_vec();
						value_cache.insert(vbytes.as_ref().to_vec(), (pk_bytes.clone(), true));
						(pk_bytes, true)
					};

					if is_new {
						changes.push((value_to_birth_key, vbytes.as_ref().to_vec(), Some(pk.clone())));
						changes.push((birth_key_to_value, pk.clone(), Some(vbytes.as_ref().to_vec())));
					}
					changes.push((key_to_birth_key, kbytes.as_ref().to_vec(), Some(pk.clone())));

					let pk_key = concat(&pk, kbytes.as_ref());
					changes.push((birth_key_key_btree, pk_key, Some(Vec::new())));
				}
				if !changes.is_empty() {
                    processed += changes.len() as u64;
					self.db.commit(changes)?;
				}
			},
		}
        if let Some(p) = self.progress.as_mut() {
            p.record(processed);
        }
        Ok(())
	}

	pub fn get_value(&self, key: &K) -> Result<Option<V>> {
		let kbytes = KC::encode(key);
		match self.layout {
			Layout::Plain { key_to_value }
			| Layout::UniqueIndex { key_to_value, .. }
			| Layout::Range { key_to_value, .. } => {
				self.db.get(key_to_value, kbytes.as_ref())?.map(|b| VC::decode(&b)).transpose()
			},
			Layout::Dictionary { key_to_birth_key, birth_key_to_value, .. } => {
				if let Some(pk) = self.db.get(key_to_birth_key, kbytes.as_ref())? {
					self.db.get(birth_key_to_value, &pk)?.map(|b| VC::decode(&b)).transpose()
				} else {
					Ok(None)
				}
			},
		}
	}

	pub fn get_key_for_value(&self, value: &V) -> Result<Option<K>> {
		let vbytes = VC::encode(value);
		match self.layout {
			Layout::UniqueIndex { value_to_key, .. } => {
				self.db.get(value_to_key, vbytes.as_ref())?.map(|b| KC::decode(&b)).transpose()
			},
			_ => Err(Error::InvalidInput("get_key_for_value not supported for this layout".into())),
		}
	}

	pub fn get_keys_for_value(&self, value: &V) -> Result<Vec<K>> {
		let vbytes = VC::encode(value);
		match self.layout {
			Layout::Range { value_key_btree, .. } => {
				let prefix = vbytes.as_ref();
				let mut out = Vec::new();
				let mut iter = self.db.iter(value_key_btree)?;
				iter.seek(prefix)?;
				while let Some((k, _)) = iter.next()? {
					if k.len() < prefix.len() || &k[..prefix.len()] != prefix {
						break
					}
					let key_bytes = &k[prefix.len()..];
					out.push(KC::decode(key_bytes)?);
				}
				Ok(out)
			},
			Layout::Dictionary { value_to_birth_key, birth_key_key_btree, .. } => {
				if let Some(pk) = self.db.get(value_to_birth_key, vbytes.as_ref())? {
					let mut iter = self.db.iter(birth_key_key_btree)?;
					iter.seek(&pk)?;
					let mut out = Vec::new();
					while let Some((k, _)) = iter.next()? {
						if k.len() < pk.len() || k[..pk.len()] != pk[..] {
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
			_ => Err(Error::InvalidInput("get_keys_for_value not supported for this layout".into())),
		}
	}

	pub fn flush(&mut self) -> Result<()> {
		Ok(())
	}
}

fn build_options(path: &Path, layout: &Layout) -> Options {
	let columns = match layout {
		Layout::Plain { .. } => 1,
		Layout::UniqueIndex { .. } => 2,
		Layout::Range { .. } => 2,
		Layout::Dictionary { .. } => 4,
	};
	let mut opts = Options::with_columns(path, columns as u8);
	for col in opts.columns.iter_mut() {
		col.uniform = false;
		col.preimage = false;
		col.compression = CompressionType::NoCompression;
	}
	if let Layout::Range { value_key_btree, .. } = layout {
		opts.columns[*value_key_btree as usize].btree_index = true;
	}
	if let Layout::Dictionary { birth_key_key_btree, .. } = layout {
		opts.columns[*birth_key_key_btree as usize].btree_index = true;
	}
	opts
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
        type Error = Error;
        type Enc<'a> = &'a [u8];
        fn encode<'a>(value: &'a Vec<u8>) -> Self::Enc<'a> {
            value.as_slice()
        }
        fn decode(bytes: &[u8]) -> Result<Vec<u8>> {
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
            Store::<Vec<u8>, Vec<u8>, BytesCodec, BytesCodec>::open_with_options(&path, Layout::unique_index(0), ()).unwrap()
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
    KC: StoreCodec<K, Error = Error>,
    VC: StoreCodec<V, Error = Error>,
{
	type Error = Error;

	fn get_value(&self, key: &K) -> Result<Option<V>> {
		Store::get_value(self, key)
	}

	fn get_key_for_value(&self, value: &V) -> Result<Option<K>> {
		Store::get_key_for_value(self, value)
	}

	fn get_keys_for_value(&self, value: &V) -> Result<Vec<K>> {
		Store::get_keys_for_value(self, value)
	}
}

impl<K, V, KC, VC> StoreWrite<K, V> for Store<K, V, KC, VC>
where
    KC: StoreCodec<K, Error = Error>,
    VC: StoreCodec<V, Error = Error>,
{
	type Options = ();
	type Layout = Layout;

	fn open_with_options(path: &Path, layout: Self::Layout, options: Self::Options) -> Result<Self> {
		Store::open_with_options(path, layout, options)
	}

	fn commit<'a, I>(&mut self, items: I) -> Result<()>
	where
		I: IntoIterator<Item = (&'a K, &'a V)>,
		K: 'a,
		V: 'a,
	{
		Store::commit(self, items)
	}

	fn flush(&mut self) -> Result<()> {
		Store::flush(self)
	}

    fn set_progress(&mut self, label: &str, total: u64) {
        self.progress = Some(ProgressTracker::new(label.to_string(), total));
    }
}
