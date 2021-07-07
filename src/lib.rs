#![feature(hash_drain_filter)]
use std::collections::HashMap;
use lru::LruCache;
use async_std::fs;

mod storage;
use storage::{Storage,FileStorage,RW};

pub type Position = (f32,f32);
pub type BBox = (f32,f32,f32,f32);
pub type QuadId = u64;
pub type RecordId = u64;
pub type Error = Box<dyn std::error::Error+Send+Sync>;

pub trait Record: Send+Sync+std::fmt::Debug {
  fn get_id(&self) -> RecordId;
  fn get_refs<'a>(&'a self) -> &'a [RecordId];
  fn get_position(&self) -> Option<Position>;
  fn lift(&self) -> Box<dyn Record>;
}

#[derive(Debug)]
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

pub struct XQ<S> where S: RW {
  storage: Box<dyn Storage<S>>,
  stores: LruCache<String,S>,
  root: QTree,
  quad_cache: LruCache<QuadId,HashMap<RecordId,Box<dyn Record>>>,
  quad_updates: HashMap<QuadId,HashMap<RecordId,Box<dyn Record>>>,
  id_cache: LruCache<RecordId,QuadId>,
  id_updates: HashMap<RecordId,QuadId>,
  missing_updates: Vec<Box<dyn Record>>,
  next_quad_id: QuadId,
}

impl<S> XQ<S> where S: RW {
  pub async fn new(storage: Box<dyn Storage<S>>) -> Result<Self,Error> {
    // todo: read tree from storage
    let mut xq = Self {
      storage,
      root: QTree::Quad {
        id: 0,
        bbox: (-180.0,-90.0,180.0,90.0),
      },
      stores: LruCache::new(500),
      quad_cache: LruCache::new(10_000),
      quad_updates: HashMap::new(),
      id_cache: LruCache::new(10_000),
      id_updates: HashMap::new(),
      missing_updates: vec![],
      next_quad_id: 1,
    };
    xq.quad_updates.insert(0, HashMap::new());
    Ok(xq)
  }
  pub async fn add_records(&mut self, records: &[Box<dyn Record>]) -> Result<(),Error> {
    let qs = self.get_quads(&records).await?;
    for (q_id,(bbox,ix)) in qs.iter() {
      for i in ix {
        let record = records.get(*i).unwrap();
        let id = record.get_id();
        self.id_updates.insert(id, *q_id);
      }
      let mut item_len = 0;
      self.quad_updates.get_mut(&q_id).map(|items| {
        for i in ix {
          let r = records.get(*i).unwrap().lift();
          items.insert(r.get_id(), r);
        }
        item_len = items.len();
        println!["{}", items.len()];
      });
      if item_len > 100_000 {
        self.split_quad(&q_id, &bbox).await?;
      }
    }
    self.check_flush().await?;
    Ok(())
  }
  pub async fn check_flush(&mut self) -> Result<(),Error> {
    // todo: call flush() if over a threshold of updates
    Ok(())
  }
  pub async fn flush(&mut self) -> Result<(),Error> {
    // todo: write to storage and clear updates and cache
    Ok(())
  }
  pub async fn get_record(&mut self, id: RecordId) -> Result<Option<Box<dyn Record>>,Error> {
    let q_id = match (self.id_updates.get(&id),self.id_cache.get(&id)) {
      (Some(q_id),_) => q_id,
      (_,Some(q_id)) => q_id,
      //(None,None) => unimplemented![], // todo: read from storage
      (None,None) => { return Ok(None) },
    };
    if let Some(records) = self.quad_updates.get(q_id) {
      return Ok(records.get(&id).map(|r| (*r).lift()));
    }
    if let Some(records) = self.quad_cache.get(q_id) {
      return Ok(records.get(&id).map(|r| (*r).lift()));
    }
    // todo: read from storage
    //unimplemented![]
    Ok(None)
  }
  async fn get_position(&mut self, record: &Box<dyn Record>) -> Result<Option<Position>,Error> {
    if let Some(p) = record.get_position() { return Ok(Some(p)) }
    let refs = record.get_refs();
    if refs.is_empty() { return Ok(None) }
    let o_r = self.get_record(*refs.first().unwrap()).await?;
    if o_r.is_none() { return Ok(None) }
    let record = o_r.unwrap();
    if let Some(p) = record.get_position() { return Ok(Some(p)) }
    let refs = record.get_refs();
    if refs.is_empty() { return Ok(None) }
    let o_r = self.get_record(*refs.first().unwrap()).await?;
    if o_r.is_none() { return Ok(None) }
    let record = o_r.unwrap();
    Ok(record.get_position())
  }
  pub async fn split_quad(&mut self, q_id: &QuadId, bbox: &BBox) -> Result<(),Error> {
    // todo: lock quad_updates and quad_cache?
    let records = match (self.quad_updates.remove(q_id),self.quad_cache.pop(q_id)) {
      (Some(rs),_) => rs,
      (_,Some(rs)) => rs,
      (None,None) => unimplemented![], // todo: read from storage
    };
    let (nx,ny) = (4,4);
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
    for (r_id,r) in records {
      if let Some(p) = self.get_position(&r).await? {
        let i = quads.iter().position(|(b,_)| overlap(&p, &b)).unwrap();
        let q = quads.get_mut(i).unwrap();
        q.1.insert(r_id,r);
      } else {
        self.missing_updates.push(r);
      }
    }
    let mut i = 0;
    for q in quads {
      if i == 0 {
        for (r_id,_) in q.1.iter() {
          self.id_updates.insert(*r_id, *q_id);
        }
        self.quad_updates.insert(*q_id, q.1);
      } else {
        let id = self.next_quad_id;
        self.next_quad_id += 1;
        for (r_id,_) in q.1.iter() {
          self.id_updates.insert(*r_id, id);
        }
        self.quad_updates.insert(id, q.1);
      }
      i += 1;
    }
    self.check_flush().await?;
    Ok(())
  }
  pub async fn get_quads(&mut self, records: &[Box<dyn Record>])
  -> Result<HashMap<QuadId,(BBox,Vec<usize>)>,Error> {
    let mut result: HashMap<QuadId,(BBox,Vec<usize>)> = HashMap::new();
    let mut positions = HashMap::new();
    for (i,r) in records.iter().enumerate() {
      if let Some(p) = self.get_position(r).await? {
        positions.insert(i,p);
      } else {
        self.missing_updates.push((*r).lift());
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
                if let Some((_,items)) = result.get_mut(id) {
                  items.push(*i);
                } else {
                  result.insert(*id, (bbox.clone(),vec![*i]));
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
    Ok(result)
  }
  pub async fn finish(&mut self) -> Result<(),Error> {
    let mut prev_len = self.missing_updates.len();
    loop {
      println!["missing.len()={}", prev_len];
      let records = self.missing_updates.drain(..).collect::<Vec<_>>();
      self.add_records(&records).await?;
      let missing_len = self.missing_updates.len();
      if missing_len == 0 || missing_len == prev_len {
        break;
      }
      prev_len = self.missing_updates.len();
    }
    println!["skipped {}", self.missing_updates.len()];
    Ok(())
  }
}

impl XQ<fs::File> {
  pub async fn open_from_path(path: &str) -> Result<XQ<fs::File>,Error> {
    Ok(Self::new(Box::new(FileStorage::open_from_path(path).await?)).await?)
  }
}

fn overlap(p: &Position, bbox: &BBox) -> bool {
  p.0 >= bbox.0 && p.0 <= bbox.2 && p.1 >= bbox.1 && p.1 <= bbox.3
}
