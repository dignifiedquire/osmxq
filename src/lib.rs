#![feature(hash_drain_filter)]

use ahash::AHashMap as HashMap;
use lru::LruCache;
use std::{sync::{RwLock,Arc,Mutex}};
use desert::{varint,ToBytes,FromBytes};
use rayon::prelude::*;

mod storage;
pub use storage::{Storage,FileStorage,RW,MmapStorage,Mmap};
mod meta;
use meta::Meta;

pub type Position = (f32,f32);
pub type BBox = (f32,f32,f32,f32);
pub type QuadId = u64;
pub type RecordId = u64;
pub type IdBlock = u64;
pub type Error = Box<dyn std::error::Error+Send+Sync>;

pub trait Record: Send+Sync+Clone+std::fmt::Debug+'static {
  fn get_id(&self) -> RecordId;
  fn get_refs(&self) -> Vec<RecordId>;
  fn get_position(&self) -> Option<Position>;
  fn pack(records: &HashMap<RecordId,Self>) -> Vec<u8> where Self: Sized;
  fn unpack(buf: &[u8], records: &mut HashMap<RecordId,Self>) -> Result<usize,Error>;
}

#[derive(Debug,Clone)]
pub enum QTree {
  Node { children: Vec<QTree>, bbox: BBox },
  Quad { id: QuadId, bbox: BBox },
}
impl QTree {
  pub fn bbox(&self) -> BBox {
    match self {
      QTree::Node { bbox, .. } => *bbox,
      QTree::Quad { bbox, .. } => *bbox,
    }
  }
}

// todo: backrefs
pub struct XQ<S,R> where S: RW, R: Record {
  storage: Arc<Mutex<Box<dyn Storage<S>>>>,
  active_files: Arc<RwLock<HashMap<String,Arc<Mutex<()>>>>>,
  root: QTree,
  record_cache: LruCache<RecordId,R>,
  quad_updates: Arc<RwLock<HashMap<QuadId,Option<HashMap<RecordId,R>>>>>,
  quad_update_age: HashMap<QuadId,usize>,
  quad_update_count_total: u64,
  quad_update_count: HashMap<QuadId,u64>, // count of staged updates in memory
  quad_count: HashMap<QuadId,u64>, // count of staged updates in memory+existing data on disk
  quad_bbox: HashMap<QuadId,BBox>,
  next_quad_id: QuadId,
  id_cache: Arc<Mutex<LruCache<RecordId,QuadId>>>,
  id_updates: Arc<RwLock<HashMap<IdBlock,HashMap<RecordId,QuadId>>>>,
  id_update_age: HashMap<IdBlock,usize>, // should be called flush age
  id_update_count_total: u64,
  id_update_count: HashMap<IdBlock,u64>,
  missing_updates: HashMap<RecordId,R>,
  missing_count: usize,
  next_missing_id: u64,
  fields: Fields,
}

#[derive(Debug,Clone)]
pub struct Fields {
  pub id_block_size: u64,
  pub id_cache_size: usize,
  pub id_flush_size: u64,
  pub id_flush_top: usize,
  pub id_flush_max_age: usize,
  pub record_cache_size: usize,
  pub quad_block_size: u64,
  pub quad_flush_size: u64,
  pub quad_flush_top: usize,
  pub quad_flush_max_age: usize,
  pub missing_flush_size: u64,
}

impl Default for Fields {
  fn default() -> Self {
    Self {
      id_block_size: 50_000,
      id_cache_size: 100_000_000,
      id_flush_size: 20_000_000,
      id_flush_top: 200,
      id_flush_max_age: 40,
      quad_block_size: 50_000,
      record_cache_size: 5_000_000,
      quad_flush_size: 5_000_000,
      quad_flush_top: 200,
      quad_flush_max_age: 40,
      missing_flush_size: 2_000_000,
    }
  }
}

impl<S,R> XQ<S,R> where S: RW, R: Record {
  pub fn open(storage: Box<dyn Storage<S>>) -> Result<Self,Error> {
    Self::from_fields(storage, Fields::default())
  }

  pub fn from_fields(mut storage: Box<dyn Storage<S>>, mut fields: Fields) -> Result<Self,Error> {
      let mfile = "meta".to_string();
      match storage.open_r(&mfile)? {
          Some(file) => {
              let buf = file.read_to_slice();
              let (_,meta) = Meta::from_bytes(&buf)?;
              fields.id_block_size = meta.id_block_size;
              let quad_bbox = Self::get_bboxes(&meta.root);
              let xq = Self {
                  storage: Arc::new(Mutex::new(storage)),
                  root: meta.root,
                  quad_bbox,
                  active_files: Arc::new(RwLock::new(HashMap::new())),
                  record_cache: LruCache::new(fields.record_cache_size),
                  quad_updates: Arc::new(RwLock::new(HashMap::new())),
                  quad_update_count_total: 0,
                  quad_update_count: HashMap::new(),
                  quad_update_age: HashMap::new(),
                  quad_count: meta.quad_count,
                  id_cache: Arc::new(Mutex::new(LruCache::new(fields.id_cache_size))),
                  id_updates: Arc::new(RwLock::new(HashMap::new())),
                  id_update_age: HashMap::new(),
                  id_update_count_total: 0,
                  id_update_count: HashMap::new(),
                  missing_updates: HashMap::new(),
                  missing_count: 0,
                  next_quad_id: meta.next_quad_id,
                  next_missing_id: 0,
                  fields,
              };
              Ok(xq)
          }
          None => {
              let mut quad_updates = HashMap::new();
              quad_updates.insert(0, None);
              let mut quad_count = HashMap::new();
              quad_count.insert(0, 0);
              let mut quad_bbox = HashMap::new();
              quad_bbox.insert(0, (-180.0,-90.0,180.0,90.0));

              let xq = Self {
                  storage: Arc::new(Mutex::new(storage)),
                  root: QTree::Quad {
                      id: 0,
                      bbox: (-180.0,-90.0,180.0,90.0),
                  },
                  quad_bbox,
                  active_files: Arc::new(RwLock::new(HashMap::new())),
                  record_cache: LruCache::new(fields.record_cache_size),
                  quad_updates: Arc::new(RwLock::new(quad_updates)),
                  quad_update_count_total: 0,
                  quad_update_count: HashMap::new(),
                  quad_update_age: HashMap::new(),
                  quad_count,
                  id_cache: Arc::new(Mutex::new(LruCache::new(fields.id_cache_size))),
                  id_updates: Arc::new(RwLock::new(HashMap::new())),
                  id_update_age: HashMap::new(),
                  id_update_count_total: 0,
                  id_update_count: HashMap::new(),
                  missing_updates: HashMap::new(),
                  missing_count: 0,
                  next_quad_id: 1,
                  next_missing_id: 0,
                  fields,
              };
              Ok(xq)
          }
      }
  }
  pub fn get_quad_ids(&self) -> Vec<QuadId> {
    let mut cursors = vec![&self.root];
    let mut ncursors = vec![];
    let mut quad_ids = vec![];
    while !cursors.is_empty() {
      ncursors.clear();
      for c in cursors.iter() {
        match c {
          QTree::Node { children, .. } => {
            ncursors.extend(children)
          },
          QTree::Quad { id, .. } => {
            quad_ids.push(*id);
          },
        }
      }
      let tmp = ncursors;
      ncursors = cursors;
      cursors = tmp;
    }
    quad_ids
  }
  fn get_bboxes(root: &QTree) -> HashMap<QuadId,BBox> {
    let mut cursors = vec![root];
    let mut ncursors = vec![];
    let mut bboxes = HashMap::new();
    while !cursors.is_empty() {
      ncursors.clear();
      for c in cursors.iter() {
        match c {
          QTree::Node { children, .. } => {
            ncursors.extend(children)
          },
          QTree::Quad { id, bbox } => {
            bboxes.insert(*id, bbox.clone());
          },
        }
      }
      let tmp = ncursors;
      ncursors = cursors;
      cursors = tmp;
    }
    bboxes
  }
  pub fn read_quad(&mut self, q_id: QuadId) -> Result<HashMap<RecordId,R>,Error> {
    let qfile = quad_file(q_id);
    let mut records = match self.open_file_r(&qfile)? {
      Some(s) => {
        let buf = s.read_to_slice();
        let mut rs = HashMap::new();
        let mut offset = 0;
        while offset < buf.len() {
          let s = R::unpack(&buf[offset..], &mut rs)?;
          offset += s;
          if s == 0 { break }
        }
        rs
      },
      None => HashMap::new(),
    };
    self.close_file(&qfile);
    for (id,r) in records.iter() {
      self.record_cache.put(*id, r.clone());
    }
    if let Some(Some(xrecords)) = self.quad_updates.read().unwrap().get(&q_id) {
      for (id,r) in xrecords {
        records.insert(*id,r.clone());
      }
    }
    Ok(records)
  }
  pub fn read_quad_denorm(&mut self, q_id: QuadId) -> Result<Vec<(RecordId,R,Vec<R>)>,Error> {
    let records = self.read_quad(q_id)?;
    let rlen = records.len();
    let mut result = Vec::with_capacity(rlen);
    for (id,record) in records.iter() {
      let refs = record.get_refs();
      let mut denorm = Vec::with_capacity(refs.len());
      for r_id in refs {
        if let Some(r) = records.get(&r_id) {
          let rrefs = r.get_refs();
          denorm.push(r.clone());
          for rr_id in rrefs {
            if let Some(rr) = records.get(&rr_id) {
              denorm.push(rr.clone());
            } else if let Some(rr) = self.get_record(rr_id)? {
              denorm.push(rr);
            }
          }
        } else if let Some(r) = self.get_record(r_id)? {
          let rrefs = r.get_refs();
          denorm.push(r.clone());
          for rr_id in rrefs {
            if let Some(rr) = records.get(&rr_id) {
              denorm.push(rr.clone());
            } else if let Some(rr) = self.get_record(rr_id)? {
              denorm.push(rr);
            }
          }
        }
        // else the ref is missing
      }
      result.push((*id,record.clone(),denorm));
    }
    Ok(result)
  }
  fn insert_id(&mut self, id: RecordId, q_id: QuadId) -> Result<(),Error> {
    let b = self.id_block(id);
    if let Some(ids) = self.id_updates.write().unwrap().get_mut(&b) {
      ids.insert(id, q_id);
      self.id_update_count_total += 1;
      if let Some(c) = self.id_update_count.get_mut(&b) {
        *c += 1;
      } else {
        self.id_update_count.insert(b, 1);
      }
      return Ok(());
    }
    {
      let mut ids = HashMap::new();
      ids.insert(id, q_id);
      self.id_updates.write().unwrap().insert(b, ids);
      self.id_update_count_total += 1;
      if let Some(c) = self.id_update_count.get_mut(&b) {
        *c += 1;
      } else {
        self.id_update_count.insert(b, 1);
      }
    }
    Ok(())
  }
  pub fn add_records(&mut self, records: &[R]) -> Result<(),Error> {
    if records.is_empty() { return Ok(()) }
    let qs = self.get_quads(&records)?;
    if qs.is_empty() { return Ok(()) }
    for (q_id,ix) in qs.iter() {
      for i in ix {
        let record = records.get(*i).unwrap();
        self.insert_id(record.get_id(), *q_id)?;
      }
      {
        let mut qu = self.quad_updates.write().unwrap();
        if let Some(Some(items)) = qu.get_mut(q_id) {
          for i in ix {
            let r = records.get(*i).unwrap();
            items.insert(r.get_id(), r.clone());
          }
        } else {
          let mut items = HashMap::new();
          for i in ix {
            let r = records.get(*i).unwrap();
            items.insert(r.get_id(), r.clone());
          }
          qu.insert(*q_id, Some(items));
        }
      }
      self.quad_add_update_count(q_id, ix.len() as u64);
      if self.quad_get_update_count(q_id) > self.fields.quad_block_size {
        self.split_quad(&q_id)?;
      }
    }
    self.check_flush()?;
    Ok(())
  }
  fn quad_add_update_count(&mut self, q_id: &QuadId, n: u64) {
    self.quad_update_count_total += n;
    if let Some(c) = self.quad_count.get_mut(q_id) {
      *c += n;
    } else {
      self.quad_count.insert(*q_id, n);
    }
    if let Some(c) = self.quad_update_count.get_mut(q_id) {
      *c += n;
    } else {
      self.quad_update_count.insert(*q_id, n);
    }
  }
  fn quad_get_update_count(&self, q_id: &QuadId) -> u64 {
    self.quad_count.get(q_id).cloned().unwrap_or(0)
  }
  pub fn check_flush(&mut self) -> Result<(),Error> {
    // todo: parallel io
    if self.quad_update_count_total >= self.fields.quad_flush_size {
      self.quad_flush_partial(self.fields.quad_flush_top)?;
    }
    if self.id_update_count_total >= self.fields.id_flush_size {
      self.id_flush_partial(self.fields.id_flush_top)?;
    }
    if self.missing_updates.len() as u64 >= self.fields.missing_flush_size {
      self.missing_flush()?;
    }
    Ok(())
  }
  pub fn quad_flush_by_id(&mut self, q_id: QuadId) -> Result<(),Error> {
    let o_rs = {
      let mut qu = self.quad_updates.write().unwrap();
      qu.remove(&q_id).and_then(|v| v)
    };
    if o_rs.is_none() { return Ok(()) }
    let rs: HashMap<u64,R> = o_rs.unwrap();
    if rs.is_empty() { return Ok(()) }
    let qfile = quad_file(q_id);
    let mut s = self.open_file_a(&qfile)?;
    let buf = R::pack(&rs);
    s.write_all(&buf)?;
    s.flush()?;
    self.close_file(&qfile);
    self.quad_update_count_total -= rs.len() as u64;
    self.quad_update_count.insert(q_id, 0);
    self.quad_update_age.insert(q_id, 0);
    Ok(())
  }

  pub fn quad_flush_by_ids(&mut self, q_ids: &[QuadId]) -> Result<(),Error> {
      let results = q_ids.par_iter().map(|q| {
      let q_id = *q;

      let o_rs = {
        let mut qu = self.quad_updates.write().unwrap();
        qu.remove(&q_id).and_then(|v| v)
      };
      if o_rs.is_none() { return Ok((q_id, 0)) }
      let rs: HashMap<u64,R> = o_rs.unwrap();
      if rs.is_empty() { return Ok((q_id, 0)) }
      let qfile = quad_file(q_id);
      let mut s = Self::open_file_a_params(&qfile, self.active_files.clone(), self.storage.clone())?;
      let buf = R::pack(&rs);
      s.write_all(&buf)?;
      s.flush()?;
      Self::close_file_params(&qfile, self.active_files.clone());

      Ok((q_id, rs.len() as u64))
    }).collect::<Vec<Result<(u64, u64), Error>>>();

    for r in results.into_iter() {
      let (q_id,n) = r?;
      self.quad_update_count_total -= n;
      self.quad_update_count.insert(q_id, 0);
      self.quad_update_age.insert(q_id, 0);
    }
    Ok(())
  }
  pub fn quad_flush_all(&mut self) -> Result<(),Error> {
    let qs = self.quad_updates.read().unwrap().keys().copied().collect::<Vec<QuadId>>();
    self.quad_flush_by_ids(&qs)
  }
  pub fn quad_flush_partial(&mut self, n: usize) -> Result<(),Error> {
    let mut qs = self.quad_updates.read().unwrap().keys()
      .filter(|q_id| { self.quad_get_update_count(q_id) > 0 })
      .copied()
      .collect::<Vec<QuadId>>();
    qs.sort_unstable_by(|a,b| {
      let ca = self.quad_get_update_count(a);
      let cb = self.quad_get_update_count(b);
      cb.cmp(&ca) // descending
    });
    self.quad_flush_by_ids(&qs[0..n.min(qs.len())])?;
    let mut queue = vec![];
    for q_id in self.quad_updates.read().unwrap().keys() {
      let age = {
        if let Some(age) = self.quad_update_age.get_mut(q_id) {
          *age += 1;
          age.clone()
        } else {
          self.quad_update_age.insert(*q_id, 1);
          1
        }
      };
      if age > self.fields.quad_flush_max_age {
        //println!["flush {} (age)", q_id];
        queue.push(*q_id);
      }
    }
    self.quad_flush_by_ids(&queue)
  }

  pub fn id_flush_by_block(&mut self, b: IdBlock) -> Result<(),Error> {
    let o_ids = self.id_updates.write().unwrap().remove(&b);
    if o_ids.is_none() { return Ok(()) }
    let ids = o_ids.unwrap();
    let ifile = id_file_from_block(b);
    let mut s = self.open_file_a(&ifile)?;
    let buf = pack_ids(&ids);
    s.write_all(&buf)?;
    s.flush()?;
    self.id_update_count_total -= ids.len() as u64;
    {
      let mut ic = self.id_cache.lock().unwrap();
      for (id,q_id) in ids {
        ic.put(id,q_id);
      }
    }
    self.id_update_count.insert(b, 0);
    self.id_update_age.insert(b, 0);
    self.close_file(&ifile);
    Ok(())
  }
  pub fn id_flush_by_blocks(&mut self, bs: &[IdBlock]) -> Result<(),Error> {
    let results = bs.par_iter().map(|br| {
      let b = *br;
      let iu = self.id_updates.clone();
      let active_files = self.active_files.clone();
      let storage = self.storage.clone();

        let o_ids = iu.write().unwrap().remove(&b);
        if o_ids.is_none() { return Ok((b,HashMap::new())) }
        let ids = o_ids.unwrap();
        let ifile = id_file_from_block(b);
        let mut s = Self::open_file_a_params(&ifile, active_files.clone(), storage)?;
        let buf = pack_ids(&ids);
        s.write_all(&buf)?;
        s.flush()?;
        Ok((b,ids))
    }).collect::<Vec<Result<_, Error>>>();
    for r in results.into_iter() {
      let (b,ids) = r?;
      self.id_update_count_total -= ids.len() as u64;
      {
        let mut ic = self.id_cache.lock().unwrap();
        for (id,q_id) in ids {
          ic.put(id,q_id);
        }
      }
      self.id_update_count.insert(b, 0);
      self.id_update_age.insert(b, 0);
      let ifile = id_file_from_block(b);
      self.close_file(&ifile);
    }
    Ok(())
  }
  pub fn id_flush_all(&mut self) -> Result<(),Error> {
    let blocks = self.id_updates.read().unwrap().keys().copied().collect::<Vec<IdBlock>>();
    self.id_flush_by_blocks(&blocks)
  }
  pub fn id_flush_partial(&mut self, n: usize) -> Result<(),Error> {
    let mut qs = self.id_updates.read().unwrap().keys()
      .filter(|b| { self.id_update_count.get(b).copied().unwrap_or(0) > 0 })
      .copied()
      .collect::<Vec<QuadId>>();
    qs.sort_unstable_by(|a,b| {
      let ca = self.id_update_count.get(a).copied().unwrap_or(0);
      let cb = self.id_update_count.get(b).copied().unwrap_or(0);
      cb.cmp(&ca) // descending
    });
    self.id_flush_by_blocks(&qs[0..n.min(qs.len())])?;
    let mut queue = vec![];
    for b in self.id_updates.read().unwrap().keys() {
      let age = {
        if let Some(age) = self.id_update_age.get_mut(b) {
          *age += 1;
          age.clone()
        } else {
          self.id_update_age.insert(*b, 1);
          1
        }
      };
      if age > self.fields.id_flush_max_age {
        queue.push(*b);
      }
    }
    self.id_flush_by_blocks(&queue)
  }

  pub fn missing_flush(&mut self) -> Result<(),Error> {
    if self.missing_updates.is_empty() { return Ok(()) }
    let m_id = self.next_missing_id;
    self.next_missing_id += 1;
    let mfile = missing_file(m_id);
    let buf = R::pack(&self.missing_updates);
    let mut s = self.open_file_rw(&mfile, buf.len() as u64)?;
    s.write_all(&buf)?;
    s.flush()?;
    self.missing_updates.clear();
    self.close_file(&mfile);
    Ok(())
  }
  pub fn flush(&mut self) -> Result<(),Error> {
    // todo: parallel io
    self.quad_flush_all()?;
    self.id_flush_all()?;
    self.missing_flush()?;
    Ok(())
  }
  fn get_qid_for_id(&mut self, id: RecordId) -> Result<Option<QuadId>,Error> {
    let b = self.id_block(id);
    let mut o_q_id = self.id_updates.read().unwrap().get(&b).and_then(|ids| ids.get(&id).copied());
    if o_q_id.is_none() {
      o_q_id = self.id_cache.lock().unwrap().get(&id).copied();
    }
    if let Some(q_id) = o_q_id {
      return Ok(Some(q_id));
    }
    let ifile = self.id_file(id);
    let mut ids = HashMap::new();

    if let Some(file) = self.open_file_r(&ifile)? {
        let buf = file.read_to_slice();
    {
      let mut offset = 0;
      while offset < buf.len() {
        let s = unpack_ids(&buf[offset..], &mut ids)?;
        offset += s;
        if s == 0 { break }
      }
    }
    }
    let g = ids.get(&id).copied();
    {
      let mut ic = self.id_cache.lock().unwrap();
      for (r_id,q_id) in ids {
        ic.put(r_id, q_id);
      }
    }
    self.close_file(&ifile);
    Ok(g)
  }
  fn get_qid_for_ids(&mut self, ids: &[RecordId]) -> Result<std::collections::HashMap<RecordId,QuadId>,Error> {
    let results = ids.par_iter().filter_map(|xid| {
      let id = *xid;
      let b = self.id_block(id);
      let ic = self.id_cache.clone();
      let iu = self.id_updates.clone();
      let ifile = self.id_file(id);
      let active_files = self.active_files.clone();
      let storage = self.storage.clone();

        let mut o_q_id = iu.read().unwrap().get(&b).and_then(|ids| ids.get(&id).copied());
        if o_q_id.is_none() {
          o_q_id = ic.lock().unwrap().get(&id).copied();
        }
        if let Some(q_id) = o_q_id {
          return Some((id,q_id));
        }
        let mut ids = HashMap::new();

        if let Some(file) = Self::open_file_r_params(&ifile, active_files.clone(), storage).unwrap() {
            let buf = file.read_to_slice();
        {
          let mut offset = 0;
          while offset < buf.len() {
            let s = unpack_ids(&buf[offset..], &mut ids).unwrap(); // TODO: fix error handling
            offset += s;
            if s == 0 { break }
          }
        }
        }
        let g = ids.get(&id).copied();
        {
          let mut icl = ic.lock().unwrap();
          for (r_id,q_id) in ids {
            icl.put(r_id, q_id);
          }
        }
        Self::close_file_params(&ifile, active_files);
        g.map(|q_id| (id,q_id))
    }).collect();
    Ok(results)
  }
  pub fn get_record(&mut self, id: RecordId) -> Result<Option<R>,Error> {
    if let Some(record) = self.record_cache.get(&id).cloned() {
      return Ok(Some(record));
    }
    let o_q_id = self.get_qid_for_id(id)?;
    if o_q_id.is_none() { return Ok(None) }
    let q_id = o_q_id.unwrap();
    if let Some(records) = self.quad_updates.read().unwrap().get(&q_id) {
      if let Some(r) = records.as_ref().and_then(|items| items.get(&id).cloned()) {
        return Ok(Some(r));
      }
    }
    let qfile = quad_file(q_id);
    let mut records = HashMap::new();

    if let Some(file) = self.open_file_r(&qfile)? {
      let buf = file.read_to_slice();
      let mut offset = 0;
    
      while offset < buf.len() {
        let s = R::unpack(&buf[offset..], &mut records)?;
        offset += s;
        if s == 0 { break }
      }
    };
    let r = records.get(&id).cloned();
    for (r_id,r) in records {
      self.record_cache.put(r_id, r);
    }
    self.close_file(&qfile);
    Ok(r)
  }
  fn get_position(&mut self, record: &R) -> Result<Option<Position>,Error> {
    if let Some(p) = record.get_position() { return Ok(Some(p)) }
    let refs = record.get_refs();
    if refs.is_empty() { return Ok(None) }
    let o_r = self.get_record(*refs.first().unwrap())?;
    if o_r.is_none() { return Ok(None) }
    let record = o_r.unwrap();
    if let Some(p) = record.get_position() { return Ok(Some(p)) }
    let refs = record.get_refs();
    if refs.is_empty() { return Ok(None) }
    let o_r = self.get_record(*refs.first().unwrap())?;
    if o_r.is_none() { return Ok(None) }
    let record = o_r.unwrap();
    Ok(record.get_position())
  }
  pub fn split_quad(&mut self, q_id: &QuadId) -> Result<(),Error> {
    let bbox = self.quad_bbox.get(q_id).copied().unwrap();
    let qfile = quad_file(*q_id);
    let records = self.read_quad(*q_id)?;
    {
      let mut st = self.storage.lock().unwrap();
      if st.exists(&qfile) {
        st.remove(&qfile)?;
      }
    }
    self.lock_file(&qfile);
    self.quad_count.insert(*q_id, 0);
    self.quad_update_count.insert(*q_id, 0);
    let (nx,ny) = (2,2);
    let mut quads = vec![];
    for i in 0..nx {
      for j in 0..ny {
        let b = (
          bbox.0 + (i as f32/(nx as f32))*(bbox.2-bbox.0),
          bbox.1 + (j as f32/(nx as f32))*(bbox.3-bbox.1),
          bbox.0 + ((i+1) as f32/(nx as f32))*(bbox.2-bbox.0),
          bbox.1 + ((j+1) as f32/(nx as f32))*(bbox.3-bbox.1),
        );
        quads.push((b,HashMap::new()));
      }
    }
    for (r_id,r) in records.iter() {
      // check in records itself which was just removed from q_id
      let mut o_p = check_position_records(r, &records);
      // then check the usual way
      if o_p.is_none() {
        o_p = self.get_position(&r)?;
      }
      if let Some(p) = o_p {
        if let Some(i) = quads.iter().position(|(b,_)| overlap(&p, &b)) {
          let q = quads.get_mut(i).unwrap();
          q.1.insert(*r_id,r.clone());
        }
      } else {
        panic!["missing record in split quad"];
        //self.missing_updates.insert(r.get_id(),r);
        //self.missing_count += 1;
      }
    }
    let mut i = 0;
    let mut nchildren = Vec::with_capacity(quads.len());
    for q in quads {
      if i == 0 {
        for (r_id,_) in q.1.iter() {
          self.insert_id(*r_id, *q_id)?;
        }
        if q.1.is_empty() {
          self.quad_updates.write().unwrap().insert(*q_id, None);
          self.quad_count.insert(*q_id, 0);
          self.quad_update_count.insert(*q_id, 0);
        } else {
          self.quad_count.insert(*q_id, q.1.len() as u64);
          self.quad_update_count.insert(*q_id, q.1.len() as u64);
          self.quad_updates.write().unwrap().insert(*q_id, Some(q.1));
        }
        self.quad_bbox.insert(*q_id, q.0.clone());
        nchildren.push(QTree::Quad { id: *q_id, bbox: q.0.clone() });
      } else {
        let id = self.next_quad_id;
        self.next_quad_id += 1;
        for (r_id,_) in q.1.iter() {
          self.insert_id(*r_id, id)?;
        }
        if q.1.is_empty() {
          self.quad_updates.write().unwrap().insert(id, None);
          self.quad_count.insert(id, 0);
          self.quad_update_count.insert(id, 0);
        } else {
          self.quad_count.insert(id, q.1.len() as u64);
          self.quad_update_count.insert(id, q.1.len() as u64);
          self.quad_updates.write().unwrap().insert(id, Some(q.1));
        }
        self.quad_bbox.insert(id, q.0.clone());
        nchildren.push(QTree::Quad { id, bbox: q.0.clone() });
      }
      i += 1;
    }

    {
      let mut cursors = vec![&mut self.root];
      let mut ncursors = vec![];
      let mut found = false;
      while !found {
        let mut count = 0;
        for c in cursors.drain(..) {
          count += 1;
          match c {
            QTree::Node { children, .. } => {
              ncursors.extend(children.iter_mut()
                .filter(|ch| { bbox_overlap(&bbox,&ch.bbox()) })
                .collect::<Vec<_>>());
            },
            QTree::Quad { id, .. } => {
              if id == q_id {
                found = true;
                *c = QTree::Node { children: nchildren.clone(), bbox: bbox.clone() };
                break;
              }
            },
          }
        }
        if count == 0 || found { break }
        cursors = ncursors.drain(..).collect();
      }
      assert![found, "did not locate quad id={}", q_id];
    }
    self.close_file(&qfile);
    self.check_flush()?;
    Ok(())
  }
  fn get_quads(&mut self, records: &[R]) -> Result<HashMap<QuadId,Vec<usize>>,Error> {
    let mut result: HashMap<QuadId,Vec<usize>> = HashMap::new();
    let mut positions = HashMap::new();

    let mut rmap = HashMap::with_capacity(records.len());
    for r in records.iter() {
      rmap.insert(r.get_id(), r.clone());
    }

    let mut get_qids = vec![];
    {
      let mut ic = self.id_cache.lock().unwrap();
      let iu = self.id_updates.read().unwrap();
      for (i,r) in records.iter().enumerate() {
        let id = r.get_id();
        let b = self.id_block(id);
        let mut o_q_id = iu.get(&b).and_then(|ids| ids.get(&id).copied());
        if o_q_id.is_none() {
          o_q_id = ic.get(&id).copied();
        }
        if let Some(q_id) = o_q_id {
          if let Some(items) = result.get_mut(&q_id) {
            items.push(i);
          } else {
            result.insert(q_id, vec![i]);
          }
        } else if let Some(p) = check_position_records(r, &rmap) {
          positions.insert(i,p);
        } else if let Some(f_id) = r.get_refs().first() {
          get_qids.push(*f_id);
        }
      }
    }
    {
      let qids = self.get_qid_for_ids(&get_qids)?;
      for (i,r) in records.iter().enumerate() {
        if positions.contains_key(&i) { continue }
        if let Some(f_id) = r.get_refs().first() {
          if let Some(q_id) = qids.get(&f_id) {
            if let Some(items) = result.get_mut(&q_id) {
              items.push(i);
            } else {
              result.insert(*q_id, vec![i]);
            }
          } else {
            self.missing_updates.insert(r.get_id(),r.clone());
            self.missing_count += 1;
          }
        }
      }
    }

    let mut cursors = vec![&self.root];
    let mut ncursors = vec![];
    while !cursors.is_empty() {
      ncursors.clear();
      for c in cursors.iter() {
        match c {
          QTree::Node { children, .. } => {
            ncursors.extend(children.iter()
              .filter(|ch| {
                positions.iter().any(|(_,p)| { overlap(p,&ch.bbox()) })
              }).collect::<Vec<_>>());
          },
          QTree::Quad { id, bbox } => {
            positions.drain_filter(|i,p| {
              if overlap(p,bbox) {
                if let Some(items) = result.get_mut(id) {
                  items.push(*i);
                } else {
                  result.insert(*id, vec![*i]);
                }
                true
              } else {
                false
              }
            });
          }
        }
      }
      let tmp = ncursors;
      ncursors = cursors;
      cursors = tmp;
    }
    //assert![positions.is_empty(), "!positions.is_empty()"];
    Ok(result)
  }
  fn lock_file(&mut self, file: &String) {
    if let Some(active) = self.active_files.read().unwrap().get(file) {
      let _lock = active.lock().unwrap();
    }
    let active = Arc::new(Mutex::new(()));
    let ac = active.clone();
    self.active_files.write().unwrap().insert(file.clone(), active);
    let _lock = ac.lock().unwrap();
  }
  fn open_file_r(&mut self, file: &String) -> Result<Option<S>,Error> {
    Self::open_file_r_params(file, self.active_files.clone(), self.storage.clone())
  }
  fn open_file_r_params(
    file: &String,
    active_files: Arc<RwLock<HashMap<String,Arc<Mutex<()>>>>>,
    storage: Arc<Mutex<Box<dyn Storage<S>>>>,
  ) -> Result<Option<S>,Error> {
    if let Some(active) = active_files.read().unwrap().get(file) {
      let _lock = active.lock().unwrap();
    }
    let mut st = storage.lock().unwrap();
    let active = Arc::new(Mutex::new(()));
    let ac = active.clone();
    active_files.write().unwrap().insert(file.clone(), active);
    let _lock = ac.lock().unwrap();
    st.open_r(file)
  }
  fn open_file_rw(&mut self, file: &String, size: u64) -> Result<S,Error> {
    if let Some(active) = self.active_files.read().unwrap().get(file) {
      let _lock = active.lock().unwrap();
    }
    let mut st = self.storage.lock().unwrap();
    let active = Arc::new(Mutex::new(()));
    let ac = active.clone();
    self.active_files.write().unwrap().insert(file.clone(), active);
    let _lock = ac.lock();
    st.open_rw(file, size)
  }
  fn open_file_a(&mut self, file: &String) -> Result<S,Error> {
    Self::open_file_a_params(
      file,
      self.active_files.clone(),
      self.storage.clone()
    )
  }
  fn open_file_a_params(
    file: &String,
    active_files: Arc<RwLock<HashMap<String,Arc<Mutex<()>>>>>,
    storage: Arc<Mutex<Box<dyn Storage<S>>>>,
  ) -> Result<S,Error> {
    if let Some(active) = active_files.read().unwrap().get(file) {
      let _lock = active.lock().unwrap();
    }
    let mut st = storage.lock().unwrap();
    let active = Arc::new(Mutex::new(()));
    let ac = active.clone();
    active_files.write().unwrap().insert(file.clone(), active);
    let _lock = ac.lock().unwrap();
    st.open_a(file)
  }
  fn close_file(&mut self, file: &String) {
    Self::close_file_params(file, self.active_files.clone())
  }
  fn close_file_params(
    file: &String,
    active_files: Arc<RwLock<HashMap<String,Arc<Mutex<()>>>>>,
  ) {
    active_files.write().unwrap().remove(file);
  }
  pub fn finish(&mut self) -> Result<(),Error> {
    let mut prev_count = 0;
    let mut missing_start = 0;
    loop {
      let missing_end = self.next_missing_id;
      let m_records = self.missing_updates.drain().map(|(_,r)| r).collect::<Vec<_>>();
      for i in missing_start..missing_end {
        let mfile = missing_file(i);
        let mut records = HashMap::new();

        if let Some(file) = self.open_file_r(&mfile)? {
            let buf = file.read_to_slice();
            {
                let mut offset = 0;
                while offset < buf.len() {
                    let s = R::unpack(&buf, &mut records)?;
                    offset += s;
                    if s == 0 { break }
                }
            }
            self.close_file(&mfile);
        }
        self.add_records(&records.drain().map(|(_,r)| r).collect::<Vec<R>>())?;
      }
      self.add_records(&m_records)?;
      if self.missing_count == 0 || self.missing_count == prev_count {
        break;
      }
      prev_count = self.missing_count;
      self.missing_count = 0;
      missing_start = missing_end;
    }
    self.missing_count = 0;
    {
      let mut st = self.storage.lock().unwrap();
      for i in 0..self.next_missing_id {
        st.remove(&missing_file(i))?;
      }
    }
    self.save_meta()?;
    Ok(())
  }
  fn get_meta(&self) -> Meta {
    Meta {
      next_quad_id: self.next_quad_id,
      id_block_size: self.fields.id_block_size,
      quad_block_size: self.fields.quad_block_size,
      root: self.root.clone(),
      quad_count: self.quad_count.clone(),
    }
  }
  pub fn save_meta(&mut self) -> Result<(),Error> {
    let mfile = "meta".to_string();
    let buf = self.get_meta().to_bytes()?;
    let mut s = self.open_file_rw(&mfile, buf.len() as u64)?;
    s.write_all(&buf)?;
    self.close_file(&mfile);
    Ok(())
  }
  fn id_block(&self, id: RecordId) -> IdBlock {
    id/(self.fields.id_block_size as u64)
  }
  fn id_file(&self, id: RecordId) -> String {
    let b = self.id_block(id);
    format!["i/{:02x}/{:x}",b%256,b/256]
  }
}

impl<R> XQ<Mmap,R> where R: Record {
  pub fn open_from_path(path: &str) -> Result<XQ<Mmap,R>,Error> {
    Ok(Self::open(Box::new(MmapStorage::open_from_path(path)?))?)
  }
}

#[inline]
fn overlap(p: &Position, bbox: &BBox) -> bool {
  p.0 >= bbox.0 && p.0 <= bbox.2 && p.1 >= bbox.1 && p.1 <= bbox.3
}
#[inline]
fn bbox_overlap(a: &BBox, b: &BBox) -> bool {
  a.2 >= b.0 && a.0 <= b.2 && a.3 >= b.1 && a.1 <= b.3
}
fn quad_file(q_id: QuadId) -> String {
  format!["q/{:02x}/{:x}",q_id%256,q_id/256]
}
fn missing_file(m_id: u64) -> String {
  format!["m/{:x}",m_id]
}
fn id_file_from_block(b: IdBlock) -> String {
  format!["i/{:02x}/{:x}",b%256,b/256]
}
fn unpack_ids(buf: &[u8], records: &mut HashMap<RecordId,QuadId>) -> Result<usize,Error> {
  if buf.is_empty() { return Ok(0) }
  let mut offset = 0;
  while offset < buf.len() {
    let (s,r_id) = varint::decode(&buf[offset..])?;
    offset += s;
    let (s,q_id) = varint::decode(&buf[offset..])?;
    offset += s;
    records.insert(r_id, q_id);
  }
  Ok(offset)
}
fn pack_ids(records: &HashMap<RecordId,QuadId>) -> Vec<u8> {
  let mut size = 0;
  for (r_id,q_id) in records {
    size += varint::length(*r_id);
    size += varint::length(*q_id);
  }
  let mut buf = vec![0;size];
  let mut offset = 0;
  for (r_id,q_id) in records {
    offset += varint::encode(*r_id, &mut buf[offset..]).unwrap();
    offset += varint::encode(*q_id, &mut buf[offset..]).unwrap();
  }
  buf
}

fn check_position_records<R: Record>(r: &R, records: &HashMap<RecordId,R>) -> Option<Position> {
  let mut o_p = r.get_position();
  if o_p.is_none() {
    o_p = r.get_refs().first()
      .and_then(|f| records.get(f))
      .and_then(|rr| rr.get_position());
  }
  if o_p.is_none() {
    o_p = r.get_refs().first()
      .and_then(|f| records.get(f))
      .and_then(|rr| rr.get_refs().first().cloned())
      .and_then(|f| records.get(&f))
      .and_then(|rr| rr.get_position());
  }
  o_p
}
