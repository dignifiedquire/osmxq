use std::{io::{Read,Write,Seek},fs,path::PathBuf};

pub trait RW: Read+Write+Seek+Truncate+Send+Sync+Unpin+'static {}
impl RW for fs::File {}

pub trait Truncate {
  fn set_len(&mut self, len: u64) -> Result<(),Error>;
}

type Error = Box<dyn std::error::Error+Send+Sync>;

pub trait Storage<S>: Send+Sync+Unpin where S: RW {
  fn open_rw(&mut self, name: &str) -> Result<S,Error>;
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
  fn open_rw(&mut self, name: &str) -> Result<fs::File,Error> {
    let p = self.path.join(name);
    fs::create_dir_all(p.parent().unwrap())?;
    Ok(fs::OpenOptions::new().read(true).write(true).create(true).open(p)?)
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

impl Truncate for fs::File {
  fn set_len(&mut self, len: u64) -> Result<(),Error> {
    fs::File::set_len(self, len).map_err(|e| e.into())
  }
}
