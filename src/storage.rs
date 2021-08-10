use std::{fs,path::PathBuf};

pub trait RW: Send+Sync {
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()>;
    fn flush(&mut self) -> std::io::Result<()>;
    fn read_to_slice(&self) -> &[u8];
}

impl RW for fs::File {
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        std::io::Write::write_all(self, buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        std::io::Write::flush(self)
    }
    fn read_to_slice(&self) -> &[u8] {
        todo!()
    }
}

type Error = Box<dyn std::error::Error+Send+Sync>;

pub trait Storage<S>: Send+Sync+Unpin where S: RW {
  fn open_rw(&mut self, name: &str, size: u64) -> Result<S,Error>;
  fn open_r(&mut self, name: &str) -> Result<Option<S>,Error>;
  fn open_a(&mut self, name: &str) -> Result<S,Error>;
  fn remove(&mut self, name: &str) -> Result<(),Error>;
  fn exists(&mut self, name: &str) -> bool;
}

pub struct FileStorage {
  path: PathBuf,
}

impl FileStorage {
  pub fn open_from_path<P>(path: P) -> Result<Self,Error>
  where PathBuf: From<P> {
    Ok(Self { path: path.into() })
  }
}

impl Storage<fs::File> for FileStorage {
  fn open_rw(&mut self, name: &str, size: u64) -> Result<fs::File,Error> {
    let p = self.path.join(name);
    fs::create_dir_all(p.parent().unwrap())?;
    let file = fs::OpenOptions::new().read(true).write(true).create(true).open(p)?;
    file.set_len(size)?;

    Ok(file)
  }
  fn open_r(&mut self, name: &str) -> Result<Option<fs::File>,Error> {
    let p = self.path.join(name);
    if p.exists() {
      Ok(Some(fs::OpenOptions::new().read(true).open(p)?))
    } else {
      Ok(None)
    }
  }
  fn open_a(&mut self, name: &str) -> Result<fs::File,Error> {
    let p = self.path.join(name);
    fs::create_dir_all(p.parent().unwrap())?;
    Ok(fs::OpenOptions::new().read(true).append(true).create(true).open(p)?)
  }
  fn remove(&mut self, name: &str) -> Result<(),Error> {
    let p = self.path.join(name);
    fs::remove_file(p).map_err(|e| e.into())
  }
  fn exists(&mut self, name: &str) -> bool {
    self.path.join(name).exists()
  }
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
    Write(memmap::MmapMut, usize),
}

impl RW for Mmap {
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Mmap::Read(_) => panic!("not a write"),
            Mmap::Write(m, offset) => {
                m[*offset..buf.len() + *offset].copy_from_slice(buf);
                *offset += buf.len();
                Ok(())
            }
        }
    }
    fn read_to_slice(&self) -> &[u8] {
        match self {
            Mmap::Read(m) => &m[..],
            Mmap::Write(m, _) => &m[..],
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Mmap::Read(_) => Ok(()),
            Mmap::Write(m, _) => m.flush(),
        }
    }
}


impl Storage<Mmap> for MmapStorage {
  fn open_rw(&mut self, name: &str, size: u64) -> Result<Mmap,Error> {
    let p = self.path.join(name);
    fs::create_dir_all(p.parent().unwrap())?;
    let file = fs::OpenOptions::new().read(true).write(true).create(true).open(p)?;
    let x = unsafe { memmap::MmapOptions::new().len(size as usize).map_mut(&file)? };

    Ok(Mmap::Write(x, 0))
  }
  fn open_r(&mut self, name: &str) -> Result<Option<Mmap>,Error> {
    let p = self.path.join(name);
    if p.exists() {
      let file = fs::OpenOptions::new().read(true).open(p)?;
        let x = unsafe {memmap::MmapOptions::new().map(&file)? };
      Ok(Some(Mmap::Read(x)))
    } else {
      Ok(None)
    }
  }
  fn open_a(&mut self, name: &str) -> Result<Mmap,Error> {
    let p = self.path.join(name);
    fs::create_dir_all(p.parent().unwrap())?;
    let file = fs::OpenOptions::new().read(true).append(true).create(true).open(p)?;
      let x = unsafe { memmap::MmapOptions::new().map_mut(&file)? };
    Ok(Mmap::Write(x, 0))
  }
  fn remove(&mut self, name: &str) -> Result<(),Error> {
    let p = self.path.join(name);
    fs::remove_file(p).map_err(|e| e.into())
  }
  fn exists(&mut self, name: &str) -> bool {
    self.path.join(name).exists()
  }
}

