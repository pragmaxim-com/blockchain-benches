use std::marker::PhantomData;

use crate::{
	bench_common::{Address, Amount, Key16, Timestamp, TxHash, KEY_LEN},
	store_interface::StoreCodec,
};

/// Supplies an error value for invalid input in codecs.
pub trait InvalidInput<E> {
	fn invalid_input(msg: &'static str) -> E;
}

pub struct KeyCodec<E, I>(PhantomData<(E, I)>);
pub struct AmountCodec<E, I>(PhantomData<(E, I)>);
pub struct TxCodec<E, I>(PhantomData<(E, I)>);
pub struct TimestampCodec<E, I>(PhantomData<(E, I)>);
pub struct AddressCodec<E>(PhantomData<E>);

impl<E: 'static, I: InvalidInput<E> + 'static> StoreCodec<Key16> for KeyCodec<E, I> {
	type Error = E;
	type Enc<'a> = &'a [u8] where E: 'a, I: 'a;
	fn encode<'a>(value: &'a Key16) -> Self::Enc<'a> {
		value.as_ref()
	}
	fn decode(bytes: &[u8]) -> Result<Key16, Self::Error> {
		let arr: [u8; KEY_LEN] = bytes.try_into().map_err(|_| I::invalid_input("bad key length"))?;
		Ok(Key16(arr))
	}
}

impl<E: 'static, I: InvalidInput<E> + 'static> StoreCodec<Amount> for AmountCodec<E, I> {
	type Error = E;
	type Enc<'a> = [u8; 8] where E: 'a, I: 'a;
	fn encode<'a>(value: &'a Amount) -> Self::Enc<'a> {
		value.0.to_le_bytes()
	}
	fn decode(bytes: &[u8]) -> Result<Amount, Self::Error> {
		let arr: [u8; 8] = bytes.try_into().map_err(|_| I::invalid_input("bad amount"))?;
		Ok(Amount(u64::from_le_bytes(arr)))
	}
}

impl<E: 'static, I: InvalidInput<E> + 'static> StoreCodec<TxHash> for TxCodec<E, I> {
	type Error = E;
	type Enc<'a> = &'a [u8] where E: 'a, I: 'a;
	fn encode<'a>(value: &'a TxHash) -> Self::Enc<'a> {
		value.as_ref()
	}
	fn decode(bytes: &[u8]) -> Result<TxHash, Self::Error> {
		let arr: [u8; 32] = bytes.try_into().map_err(|_| I::invalid_input("bad tx hash"))?;
		Ok(TxHash(arr))
	}
}

impl<E: 'static, I: InvalidInput<E> + 'static> StoreCodec<Timestamp> for TimestampCodec<E, I> {
	type Error = E;
	type Enc<'a> = [u8; 8] where E: 'a, I: 'a;
	fn encode<'a>(value: &'a Timestamp) -> Self::Enc<'a> {
		value.0.to_le_bytes()
	}
	fn decode(bytes: &[u8]) -> Result<Timestamp, Self::Error> {
		let arr: [u8; 8] = bytes.try_into().map_err(|_| I::invalid_input("bad timestamp"))?;
		Ok(Timestamp(u64::from_le_bytes(arr)))
	}
}

impl<E: 'static> StoreCodec<Address> for AddressCodec<E> {
	type Error = E;
	type Enc<'a> = &'a [u8] where E: 'a;
	fn encode<'a>(value: &'a Address) -> Self::Enc<'a> {
		value.as_ref()
	}
	fn decode(bytes: &[u8]) -> Result<Address, Self::Error> {
		Ok(Address(bytes.to_vec()))
	}
}
