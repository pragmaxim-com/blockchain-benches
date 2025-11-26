use fst::{IntoStreamer, Map, MapBuilder, Streamer};
use memmap2::Mmap;
use std::{
	cmp::Reverse,
	collections::{BinaryHeap, BTreeMap, HashSet},
	fs::{self, File},
	io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
	path::{Path, PathBuf},
};

use crate::fst::store::{StoreError, StoreResult};

pub struct Segment {
	pub(crate) id: u64,
	pub(crate) map: Map<Mmap>,
	pub(crate) values_path: PathBuf,
}

#[derive(Clone)]
pub(crate) struct SegmentMeta {
	pub(crate) id: u64,
	pub(crate) fst_path: PathBuf,
	pub(crate) values_path: PathBuf,
}

pub struct Column {
	pub(crate) id: u8,
	pub(crate) dir: PathBuf,
	pub(crate) memtable: BTreeMap<Vec<u8>, Vec<u8>>,
	pub(crate) segments: Vec<Segment>,
	pub(crate) next_segment_id: u64,
	pub(crate) segment_size: usize,
	pub(crate) merging: bool,
}

impl Column {
	pub(crate) fn open(dir: &Path, id: u8, segment_size: usize) -> StoreResult<Self> {
		let mut segments = load_segments(dir, id)?;
		segments.sort_by_key(|s| s.id);
		let next_segment_id = segments.last().map(|s| s.id + 1).unwrap_or(0);
		Ok(Self {
			id,
			dir: dir.to_path_buf(),
			memtable: BTreeMap::new(),
			segments,
			next_segment_id,
			segment_size,
			merging: false,
		})
	}

	pub(crate) fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> StoreResult<bool> {
		self.memtable.insert(key, value);
		if self.memtable.len() >= self.segment_size {
			self.flush()?;
			return Ok(true)
		}
		Ok(false)
	}

	pub(crate) fn flush(&mut self) -> StoreResult<()> {
		if self.memtable.is_empty() {
			return Ok(())
		}
		let seg_id = self.next_segment_id;
		let (fst_path, values_path) = segment_paths(&self.dir, self.id, seg_id);
		let fst_file = BufWriter::new(File::create(&fst_path)?);
		let mut map_builder = MapBuilder::new(fst_file)?;
		let mut val_writer = BufWriter::new(File::create(&values_path)?);
		let mut offset: u64 = 0;
		for (key, value) in self.memtable.iter() {
			map_builder.insert(key, offset)?;
			write_value(&mut val_writer, value)?;
			offset = offset.checked_add(4 + value.len() as u64).ok_or_else(|| {
				StoreError::InvalidInput("value offsets exceeded u64".into())
			})?;
		}
		map_builder.finish()?;
		val_writer.flush()?;
		let file = File::open(&fst_path)?;
		let mmap = unsafe { Mmap::map(&file)? };
		let map = Map::new(mmap)?;
		self.segments.push(Segment { id: seg_id, map, values_path });
		self.next_segment_id += 1;
		self.memtable.clear();
		Ok(())
	}

	pub(crate) fn get(&self, key: &[u8]) -> StoreResult<Option<Vec<u8>>> {
		if let Some(v) = self.memtable.get(key) {
			return Ok(Some(v.clone()))
		}
		for seg in self.segments.iter().rev() {
			if let Some(offset) = seg.map.get(key) {
				return Ok(Some(seg.read_value(offset)?))
			}
		}
		Ok(None)
	}

	pub(crate) fn keys_with_prefix(&self, prefix: &[u8]) -> StoreResult<Vec<Vec<u8>>> {
		let mut seen: HashSet<Vec<u8>> = HashSet::new();
		let mut keys: Vec<Vec<u8>> = Vec::new();
		let range_end = prefix_upper_bound(prefix);

		if let Some(end) = range_end.as_ref() {
			for (k, _) in self.memtable.range(prefix.to_vec()..end.clone()) {
				if k.starts_with(prefix) && seen.insert(k.clone()) {
					keys.push(k.clone());
				}
			}
		} else {
			for (k, _) in self.memtable.range(prefix.to_vec()..) {
				if !k.starts_with(prefix) {
					break;
				}
				if seen.insert(k.clone()) {
					keys.push(k.clone());
				}
			}
		}

		for seg in self.segments.iter().rev() {
			let mut builder = seg.map.range().ge(prefix);
			if let Some(end) = range_end.as_deref() {
				builder = builder.lt(end);
			}
			let mut stream = builder.into_stream();
			while let Some((key, _)) = stream.next() {
				if range_end.is_none() && !key.starts_with(prefix) {
					break;
				}
				if seen.insert(key.to_vec()) {
					keys.push(key.to_vec());
				}
			}
		}

		keys.sort();
		Ok(keys)
	}

	pub(crate) fn multi_way_merge(&mut self) -> StoreResult<()> {
		self.flush()?;
		if self.segments.len() <= 1 {
			return Ok(())
		}
		let merge_id = self.next_segment_id;
		self.next_segment_id += 1;
		let snapshot = std::mem::take(&mut self.segments);
		let metas: Vec<SegmentMeta> = snapshot
			.iter()
			.map(|s| {
				let (fst_path, values_path) = segment_paths(&self.dir, self.id, s.id);
				SegmentMeta { id: s.id, fst_path, values_path }
			})
			.collect();
		let (merged, old_meta) = merge_segments(&self.dir, self.id, merge_id, metas)?;
		self.segments.push(merged);
		for m in old_meta {
			let _ = fs::remove_file(m.fst_path);
			let _ = fs::remove_file(m.values_path);
		}
		Ok(())
	}

	pub(crate) fn snapshot_for_merge(&mut self, threshold: usize) -> StoreResult<Option<(u64, PathBuf, u8, Vec<SegmentMeta>)>> {
		if self.merging {
			return Ok(None)
		}
		self.flush()?;
		if self.segments.len() < threshold {
			return Ok(None)
		}
		let merge_id = self.next_segment_id;
		self.next_segment_id += 1;
		let metas: Vec<SegmentMeta> = self
			.segments
			.iter()
			.map(|s| {
				let (fst_path, values_path) = segment_paths(&self.dir, self.id, s.id);
				SegmentMeta { id: s.id, fst_path, values_path }
			})
			.collect();
		self.merging = true;
		Ok(Some((merge_id, self.dir.clone(), self.id, metas)))
	}

	pub(crate) fn finish_merge(&mut self, merged: Segment, old_meta: &[SegmentMeta]) {
		self.segments.retain(|s| !old_meta.iter().any(|m| m.id == s.id));
		self.segments.push(merged);
		self.merging = false;
		for m in old_meta {
			let _ = fs::remove_file(&m.fst_path);
			let _ = fs::remove_file(&m.values_path);
		}
	}
}

impl Segment {
	pub(crate) fn read_value(&self, offset: u64) -> StoreResult<Vec<u8>> {
		read_value_from_path(&self.values_path, offset)
	}
}

pub(crate) fn merge_segments(dir: &Path, col_id: u8, new_id: u64, metas: Vec<SegmentMeta>) -> StoreResult<(Segment, Vec<SegmentMeta>)> {
	let mut holders = Vec::with_capacity(metas.len());
	for m in &metas {
		let file = File::open(&m.fst_path)?;
		let mmap = unsafe { Mmap::map(&file)? };
		let map = Map::new(mmap)?;
		holders.push((map, m.values_path.clone(), m.id));
	}

	let mut streams: Vec<_> = holders.iter().map(|(map, _, _)| map.stream()).collect();
	let mut value_readers: Vec<_> = holders
		.iter()
		.map(|(_, val_path, _)| ValueReader::new(File::open(val_path).unwrap()))
		.collect();
	let mut heap: BinaryHeap<Reverse<(Vec<u8>, Reverse<u64>, usize, u64)>> = BinaryHeap::new();
	for (idx, stream) in streams.iter_mut().enumerate() {
		if let Some((k, off)) = stream.next() {
			let seg_id = holders[idx].2;
			heap.push(Reverse((k.to_vec(), Reverse(seg_id), idx, off)));
		}
	}

	let (fst_path, values_path) = segment_paths(dir, col_id, new_id);
	let mut map_builder = MapBuilder::new(BufWriter::new(File::create(&fst_path)?))?;
	let mut val_writer = BufWriter::new(File::create(&values_path)?);
	let mut write_offset: u64 = 0;
	let mut last_emitted: Option<Vec<u8>> = None;

	while let Some(Reverse((key, _sid, seg_idx, val_offset))) = heap.pop() {
		if last_emitted.as_ref().map_or(false, |prev| *prev == key) {
			if let Some((k, off)) = streams[seg_idx].next() {
				let seg_id = holders[seg_idx].2;
				heap.push(Reverse((k.to_vec(), Reverse(seg_id), seg_idx, off)));
			}
			continue;
		}
		let val = value_readers[seg_idx].read_at(val_offset)?;
		map_builder.insert(&key, write_offset)?;
		write_value(&mut val_writer, &val)?;
		let next_offset = write_offset.checked_add(4 + val.len() as u64).ok_or_else(|| {
			StoreError::InvalidInput("value offsets exceeded u64".into())
		})?;
		write_offset = next_offset;
		last_emitted = Some(key.clone());

		if let Some((k, off)) = streams[seg_idx].next() {
			let seg_id = holders[seg_idx].2;
			heap.push(Reverse((k.to_vec(), Reverse(seg_id), seg_idx, off)));
		}
	}

	map_builder.finish()?;
	val_writer.flush()?;
	let file = File::open(&fst_path)?;
	let mmap = unsafe { Mmap::map(&file)? };
	let map = Map::new(mmap)?;
	let new_seg = Segment { id: new_id, map, values_path };

	Ok((new_seg, metas))
}

pub(crate) fn load_segments(dir: &Path, col_id: u8) -> StoreResult<Vec<Segment>> {
	let mut segments = Vec::new();
	let prefix = format!("col{col_id}_seg");
	for entry in fs::read_dir(dir)? {
		let entry = entry?;
		let fname = entry.file_name();
		let fname = match fname.to_str() {
			Some(f) => f,
			None => continue,
		};
		if !fname.starts_with(&prefix) || !fname.ends_with(".fst") {
			continue
		}
		let id_part = &fname[prefix.len()..fname.len() - 4];
		let id: u64 = match id_part.parse() {
			Ok(id) => id,
			Err(_) => continue,
		};
		let fst_path = dir.join(fname);
		let values_path = dir.join(format!("col{col_id}_seg{id_part}.val"));
		if !values_path.exists() {
			return Err(StoreError::CorruptSegment(format!("missing values file for {}", fname)))
		}
		let file = File::open(&fst_path)?;
		let mmap = unsafe { Mmap::map(&file)? };
		let map = Map::new(mmap)?;
		segments.push(Segment { id, map, values_path });
	}
	Ok(segments)
}

pub(crate) fn segment_paths(dir: &Path, col: u8, id: u64) -> (PathBuf, PathBuf) {
	let name = format!("col{col}_seg{id:020}");
	(dir.join(format!("{name}.fst")), dir.join(format!("{name}.val")))
}

pub(crate) fn write_value<W: Write>(writer: &mut W, value: &[u8]) -> StoreResult<()> {
	let len = u32::try_from(value.len()).map_err(|_| StoreError::InvalidInput("value too large".into()))?;
	writer.write_all(&len.to_le_bytes())?;
	writer.write_all(value)?;
	Ok(())
}

pub(crate) fn read_value_from_path(path: &Path, offset: u64) -> StoreResult<Vec<u8>> {
	let mut file = File::open(path)?;
	file.seek(SeekFrom::Start(offset))?;
	let mut len_buf = [0u8; 4];
	file.read_exact(&mut len_buf)?;
	let len = u32::from_le_bytes(len_buf) as usize;
	let mut buf = vec![0u8; len];
	file.read_exact(&mut buf)?;
	Ok(buf)
}

struct ValueReader {
	reader: BufReader<File>,
	pos: u64,
}

impl ValueReader {
	fn new(file: File) -> Self {
		Self { reader: BufReader::new(file), pos: 0 }
	}

	fn read_at(&mut self, offset: u64) -> StoreResult<Vec<u8>> {
		if self.pos != offset {
			self.reader.seek(SeekFrom::Start(offset))?;
			self.pos = offset;
		}
		let mut len_buf = [0u8; 4];
		self.reader.read_exact(&mut len_buf)?;
		let len = u32::from_le_bytes(len_buf) as usize;
		let mut buf = vec![0u8; len];
		self.reader.read_exact(&mut buf)?;
		self.pos = self.pos.checked_add(4 + len as u64).unwrap_or(self.pos);
		Ok(buf)
	}
}

pub(crate) fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
	if prefix.is_empty() {
		return None
	}
	let mut out = prefix.to_vec();
	for i in (0..out.len()).rev() {
		if out[i] != u8::MAX {
			out[i] += 1;
			out.truncate(i + 1);
			return Some(out)
		}
	}
	None
}

#[cfg(test)]
mod tests {
	use super::*;
	use tempfile::tempdir;

	#[test]
	fn flushes_and_reads_single_segment() {
		let dir = tempdir().unwrap();
		let mut col = Column::open(dir.path(), 0, 2).unwrap();
		col.insert(b"a".to_vec(), b"1".to_vec()).unwrap();
		col.insert(b"b".to_vec(), b"2".to_vec()).unwrap();
		col.flush().unwrap();
		assert_eq!(col.segments.len(), 1);
		assert_eq!(col.get(b"a").unwrap(), Some(b"1".to_vec()));
		assert_eq!(col.get(b"b").unwrap(), Some(b"2".to_vec()));
	}

	#[test]
	fn multi_way_merge_prefers_newer_segment() {
		let dir = tempdir().unwrap();
		let mut col = Column::open(dir.path(), 0, 1).unwrap();
		col.insert(b"k".to_vec(), b"old".to_vec()).unwrap();
		col.insert(b"k".to_vec(), b"new".to_vec()).unwrap();
		col.flush().unwrap();
		assert!(col.segments.len() >= 2);
		col.multi_way_merge().unwrap();
		assert_eq!(col.segments.len(), 1);
		assert_eq!(col.get(b"k").unwrap(), Some(b"new".to_vec()));
	}

	#[test]
	fn keys_with_prefix_dedupes_from_segments() {
		let dir = tempdir().unwrap();
		let mut col = Column::open(dir.path(), 0, 1).unwrap();
		col.insert(b"p1".to_vec(), vec![]).unwrap();
		col.insert(b"p2".to_vec(), vec![]).unwrap();
		col.insert(b"p1".to_vec(), vec![]).unwrap(); // newer duplicate
		col.flush().unwrap();
		let keys = col.keys_with_prefix(b"p").unwrap();
		assert_eq!(keys, vec![b"p1".to_vec(), b"p2".to_vec()]);
	}
}
