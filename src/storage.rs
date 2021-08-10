use std::{fs,path::PathBuf, ops::{Deref, DerefMut}};

pub trait RW: Send+Sync+Deref<Target=[u8]>+DerefMut<Target=[u8]> {
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()>;
    fn flush(&mut self) -> std::io::Result<()>;
    fn read_to_slice(&self) -> &[u8];
    fn set_len(&mut self, len: u64) -> std::io::Result<()>;
    fn set_offset(&mut self, offset: usize);
}

type Error = Box<dyn std::error::Error+Send+Sync>;

pub trait Storage<S>: Send+Sync+Unpin where S: RW {
  fn open_rw(&mut self, name: &str, size: u64) -> Result<S,Error>;
  fn open_r(&mut self, name: &str) -> Result<Option<S>,Error>;
  fn open_a(&mut self, name: &str) -> Result<S,Error>;
  fn remove(&mut self, name: &str) -> Result<(),Error>;
  fn exists(&mut self, name: &str) -> bool;
}

pub struct MmapStorage {
  path: PathBuf,
}

impl MmapStorage {
  pub fn open_from_path<P>(path: P) -> Result<Self,Error>
  where PathBuf: From<P> {
    Ok(Self { path: path.into() })
  }
}

pub enum Mmap {
    Read(memmap::Mmap),
    Write(memmap::MmapMut, PathBuf, usize),
}
impl Drop for Mmap {
    fn drop(&mut self) {
        match self {
            Mmap::Read(_) => {},
            Mmap::Write(m, _, _) => {
                m.flush().unwrap();
            }
        }
    }
}

impl Deref for Mmap {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            Mmap::Read(m) => &m[..],
            Mmap::Write(m, _, offset) => &m[*offset..],
        }
    }
}

impl DerefMut for Mmap {
    fn deref_mut(&mut self) -> &mut [u8] {
        match self {
            Mmap::Read(_m) => panic!("not a write"),
            Mmap::Write(m, _, offset) => &mut m[*offset..],
        }
    }
}

impl RW for Mmap {
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Mmap::Read(_) => panic!("not a write"),
            Mmap::Write(m, _, offset) => {
                m[*offset..buf.len() + *offset].copy_from_slice(buf);
                *offset += buf.len();

                //m.flush_range(*offset, buf.len())?;
                Ok(())
            }
        }
    }
    fn read_to_slice(&self) -> &[u8] {
        match self {
            Mmap::Read(m) => &m[..],
            Mmap::Write(m, _, _) => &m[..],
        }
    }
    fn set_len(&mut self, len: u64) -> std::io::Result<()> {
        match self {
            Mmap::Read(_) => panic!("not a write"),
            Mmap::Write(m, p, _) => {
                let file = fs::OpenOptions::new().read(true).write(true).create(true).open(p)?;
                file.set_len(len)?;
                *m = unsafe { memmap::MmapOptions::new().len(len as usize).map_mut(&file)? };
                Ok(())
            }
        }
    }
    fn set_offset(&mut self, new_offset: usize) {
        match self {
            Mmap::Read(_) => panic!("not a write"),
            Mmap::Write(_m, _, offset) => *offset = new_offset,
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Mmap::Read(_) => panic!("not a write"),
            Mmap::Write(m, _, _) => m.flush(),
        }
    }
}


impl Storage<Mmap> for MmapStorage {
  fn open_rw(&mut self, name: &str, size: u64) -> Result<Mmap,Error> {
    let p = self.path.join(name);
    fs::create_dir_all(p.parent().unwrap())?;
      let file = fs::OpenOptions::new().read(true).write(true).create(true).open(&p)?;
      let size = size.max(1024);
      file.set_len(size)?;
    let x = unsafe { memmap::MmapOptions::new().len(size as usize).map_mut(&file)? };

    Ok(Mmap::Write(x, p, 0))
  }
  fn open_r(&mut self, name: &str) -> Result<Option<Mmap>,Error> {
    let p = self.path.join(name);
    if p.exists() {
      let file = fs::OpenOptions::new().read(true).open(p)?;
      let size = file.metadata()?.len();
      let x = unsafe {memmap::MmapOptions::new().len(size as usize).map(&file)? };

      Ok(Some(Mmap::Read(x)))
    } else {
      Ok(None)
    }
  }
  fn open_a(&mut self, name: &str) -> Result<Mmap,Error> {
    let p = self.path.join(name);
    fs::create_dir_all(p.parent().unwrap())?;
    let file = fs::OpenOptions::new().read(true).append(true).create(true).open(&p)?;
      if file.metadata()?.len() == 0 {
          file.set_len(1024)?;
      }
    let size = file.metadata()?.len();
    let x = unsafe { memmap::MmapOptions::new().len(size as usize).map_mut(&file)? };
    Ok(Mmap::Write(x, p, 0))
  }
  fn remove(&mut self, name: &str) -> Result<(),Error> {
    let p = self.path.join(name);
    fs::remove_file(p).map_err(|e| e.into())
  }
  fn exists(&mut self, name: &str) -> bool {
    self.path.join(name).exists()
  }
}

