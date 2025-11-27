use fst::Map;
use memmap2::Mmap;
use std::{
	fs::File,
	path::Path,
	sync::{mpsc, Arc, RwLock},
	thread,
	time::Instant,
};

use crate::segment::{merge_segments, Column};
use crate::store::StoreResult;

const MERGE_THRESHOLD: usize = 4;

pub struct Compactor {
	handle: Option<thread::JoinHandle<()>>,
	sender: Option<mpsc::Sender<usize>>,
}

impl Compactor {
	pub fn new(columns: Vec<Arc<RwLock<Column>>>) -> Self {
		let (tx, rx) = mpsc::channel::<usize>();
		let handle = thread::spawn(move || {
			while let Ok(idx) = rx.recv() {
				if let Some(col) = columns.get(idx) {
					let snapshot = {
						let mut guard = col.write().unwrap();
						match guard.snapshot_for_merge(MERGE_THRESHOLD) {
							Ok(Some(s)) => s,
							Ok(None) => continue,
							Err(e) => {
								eprintln!("compaction col {} snapshot error: {}", idx, e);
								continue
							},
						}
					};

					let (merge_id, dir, col_id, metas) = snapshot;
					let before_rows: u64 = metas.iter().map(|m| read_rows(&m.fst_path)).sum();
					let start = Instant::now();
					match merge_segments(&dir, col_id, merge_id, metas.clone()) {
						Ok((merged, metas_back)) => {
							let dur = start.elapsed();
							let after_rows = merged.map.len() as u64;
							let ops = if dur.as_secs_f64() > 0.0 { before_rows as f64 / dur.as_secs_f64() } else { 0.0 };
							if let Ok(mut guard) = col.write() {
								guard.finish_merge(merged, &metas_back);
							}
							println!(
								"compaction col {}: segs {}->{} rows {}->{} in {:.2?} (~{:.1} rows/s)",
								idx,
								metas_back.len(),
								1,
								before_rows,
								after_rows,
								dur,
								ops
							);
						},
						Err(e) => {
							eprintln!("compaction col {} merge error: {}", idx, e);
							if let Ok(mut guard) = col.write() {
								guard.merging = false;
							}
						},
					}
				}
			}
		});
		Self { sender: Some(tx), handle: Some(handle) }
	}

	pub fn request(&self, col_idx: usize) -> StoreResult<()> {
		if let Some(sender) = &self.sender {
			let _ = sender.send(col_idx);
		}
		Ok(())
	}
}

impl Drop for Compactor {
	fn drop(&mut self) {
		self.sender.take();
		if let Some(h) = self.handle.take() {
			let _ = h.join();
		}
	}
}

fn read_rows(path: &Path) -> u64 {
	match File::open(path)
		.ok()
		.and_then(|f| unsafe { Mmap::map(&f).ok() })
		.and_then(|m| Map::new(m).ok())
	{
		Some(map) => map.len() as u64,
		None => 0,
	}
}
