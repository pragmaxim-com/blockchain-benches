use std::path::Path;

/// Borrow-friendly codec shared by store implementations.
pub trait StoreCodec<T> {
	type Error;
	type Enc<'a>: AsRef<[u8]> where T: 'a, Self: 'a;
	fn encode<'a>(value: &'a T) -> Self::Enc<'a>;
	fn decode(bytes: &[u8]) -> Result<T, Self::Error>;
}

pub trait StoreRead<K, V> {
	type Error;
	fn get_value(&self, key: &K) -> Result<Option<V>, Self::Error>;
	fn get_key_for_value(&self, value: &V) -> Result<Option<K>, Self::Error>;
	fn get_keys_for_value(&self, value: &V) -> Result<Vec<K>, Self::Error>;
}

pub trait StoreWrite<K, V>: StoreRead<K, V> {
	type Options: Default;
	type Layout: Copy;

	fn open_with_options(path: &Path, layout: Self::Layout, options: Self::Options) -> Result<Self, Self::Error>
	where
		Self: Sized;

	fn commit<'a, I>(&mut self, items: I) -> Result<(), Self::Error>
	where
		I: IntoIterator<Item = (&'a K, &'a V)>,
		K: 'a,
		V: 'a;

	fn flush(&mut self) -> Result<(), Self::Error>;

	fn set_progress(&mut self, _label: &str, _total: u64) {}
}


pub struct ProgressTracker {
    label: String,
    total: u64,
    inserted: u64,
    start: std::time::Instant,
    last_report: std::time::Instant,
}

impl ProgressTracker {
    pub fn new(label: String, total: u64) -> Self {
        let now = std::time::Instant::now();
        Self { label, total, inserted: 0, start: now, last_report: now }
    }

    pub fn record(&mut self, delta: u64) {
        self.inserted = self.inserted.saturating_add(delta);
        let now = std::time::Instant::now();
        if now.duration_since(self.last_report).as_secs() >= 5 {
            let elapsed = now.duration_since(self.start);
            let speed = self.inserted as f64 / elapsed.as_secs_f64();
            println!("{}: progress {}/{} (~{:.1} rows/s)", self.label, self.inserted, self.total, speed);
            self.last_report = now;
        }
    }
}
