use std::env::temp_dir;
use std::path::PathBuf;

use crate::error::Error;
use crate::error::Result;

pub struct PagerOption {
    /// db dir
    pub path: Option<&'static str>,
    /// max page
    pub max_page: u32,
    /// the page extra data size
    pub n_extra: u64,
    /// the db is read_only
    pub read_only: bool,
}

impl PagerOption {
    /// get fd,jfd path
    pub fn get_paths(&self) -> Result<(PathBuf, PathBuf)> {
        match self.path {
            Some(dir) => {
                let mut dir_path = PathBuf::from(dir);
                if dir_path.is_file() {
                    return Err(Error::Value(format!("DB Path {} is not a dir path", dir)));
                }
                dir_path.push("kvdb.db");
                let z_filename = PathBuf::from(dir_path.as_path());
                dir_path.pop();
                dir_path.push("kvdb.journal");
                let z_journal = PathBuf::from(dir_path.as_path());
                Ok((z_filename, z_journal))
            }
            None => {
                let z_filename = temp_dir().join("kvdb.temp.db");
                let z_journal = temp_dir().join("kvdb.temp.journal");
                Ok((z_filename, z_journal))
            }
        }
    }

    pub fn is_temp(&self) -> bool {
        self.path.is_none()
    }

    pub fn get_mx_path(&self) -> u32 {
        if self.max_page > 5 {
            self.max_page
        } else {
            10
        }
    }
}
