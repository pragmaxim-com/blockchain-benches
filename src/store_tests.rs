use std::fmt::Debug;

use crate::store_interface::StoreWrite;

/// Basic put/get/overwrite cycle for a store using `Vec<u8>` keys and values.
pub fn basic_value_roundtrip<S, F>(mut factory: F)
where
	S: StoreWrite<Vec<u8>, Vec<u8>>,
	S::Error: Debug,
	F: FnMut() -> S,
{
	let mut store = factory();
	let k = b"key".to_vec();
	let v1 = b"value1".to_vec();
	let v2 = b"value2".to_vec();

	store.commit([(&k, &v1)]).expect("commit");
	assert_eq!(store.get_value(&k).expect("get"), Some(v1.clone()));
	store.flush().expect("flush");
	assert_eq!(store.get_value(&k).expect("get after flush"), Some(v1.clone()));

	store.commit([(&k, &v2)]).expect("overwrite commit");
	assert_eq!(store.get_value(&k).expect("get overwrite"), Some(v2.clone()));
	store.flush().expect("flush overwrite");
	assert_eq!(store.get_value(&k).expect("get overwrite after flush"), Some(v2));
}

/// Reverse lookup test for stores supporting a unique value->key mapping.
pub fn reverse_lookup_unique<S, F>(mut factory: F)
where
	S: StoreWrite<Vec<u8>, Vec<u8>>,
	S::Error: Debug,
	F: FnMut() -> S,
{
	let mut store = factory();
	let k = b"k".to_vec();
	let v = b"val".to_vec();
	store.commit([(&k, &v)]).expect("commit");
	assert_eq!(store.get_key_for_value(&v).expect("reverse get"), Some(k.clone()));
	store.flush().expect("flush");
	assert_eq!(store.get_key_for_value(&v).expect("reverse after flush"), Some(k));
}

/// Multi-key lookup for stores supporting range/dictionary style value->keys.
pub fn multiple_keys_for_value<S, F>(mut factory: F)
where
	S: StoreWrite<Vec<u8>, Vec<u8>>,
	S::Error: Debug,
	F: FnMut() -> S,
{
	let mut store = factory();
	let v = b"shared".to_vec();
	let keys = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
	for k in &keys {
		store.commit([ (k, &v) ]).expect("commit");
	}
	store.flush().expect("flush");
	let mut got = store.get_keys_for_value(&v).expect("get keys");
	got.sort();
	assert_eq!(got, keys);
}
