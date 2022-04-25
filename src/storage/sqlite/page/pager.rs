use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;

use derivative::Derivative;

use super::page_error::error_values;
use super::page_error::SQLExecValue;
use super::PgHdr;
use crate::common::options::PagerOption;
use crate::error::Result;

/// page size
pub(super) const PAGE_SIZE: usize = 1024;

#[derive(Debug, PartialEq, Eq)]
enum PageLockState {
    UNLOCK,
    READLOCK,
    WRITELOCK,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct Pager {
    /// Path of database file
    z_filename: PathBuf,
    // Path of journal file
    z_journal: PathBuf,
    // file descriptor for database
    fd: Option<RwLock<File>>,
    // file descriptor for journal
    jfd: Option<RwLock<File>>,
    // file descriptor for checkpoint journal
    cpfd: Option<RwLock<File>>,
    // number of pages in the database filename
    db_size: u32,
    // db_size before the current change
    orig_db_size: u32,
    // size of database at ckpt_begine()
    ckpt_size: u64,
    // size of journal at ckpt_begine()
    ckpt_j_size: u64,
    // add this many bytes to each in-memory page, the user extra data size
    n_extra: u64,
    // total number of in-memory pages
    n_page: u32,
    // number of in-memory page with PgHdr.n_ref > 0
    n_ref: u32,
    // max number of pages to hold in cache
    mx_page: u32,
    // cache hits
    n_hit: u32,
    // cache miss
    n_miss: u32,
    // LRU overflows
    n_ovfl: u32,
    // true if journal file descriptor is valid
    journal_open: bool,
    // true if the checkpoint journal is open
    ckpt_open: bool,
    // true if we a in a checkpoint
    ckpt_in_use: bool,
    // true if write the database and flush disk
    no_sync: bool,
    // the lock state
    stats: PageLockState,
    // one of several kinds of errors, error msg
    err_mask: u8,
    // true if the z_filename is a temporary file
    temp_file: bool,
    // true if the database readonly
    read_only: bool,
    // true if flush disk on the journal before write to the databse
    need_sync: bool,
    // true if database file has changed in any way
    dirty_file: bool,
    // one bit for each page in the database file
    a_in_journal: Option<Vec<u8>>,
    // one bit for each page in the database
    a_in_ckpt: Option<Vec<u8>>,
    // list of free page
    p_first: Option<Arc<Mutex<PgHdr>>>,
    // list of free page
    p_last: Option<Arc<Mutex<PgHdr>>>,
    // list of all pages
    p_all: Option<Arc<Mutex<PgHdr>>>,
    // hash table to map page number of PgHdr
    #[derivative(Debug = "ignore")]
    a_hash: HashMap<u32, Arc<Mutex<PgHdr>>>,
}

impl Pager {
    pub fn open(option: PagerOption) -> Result<Arc<Mutex<Self>>> {
        let (z_filename, z_journal) = option.get_paths()?;
        let pager = Pager {
            z_filename,
            z_journal,
            fd: None,
            jfd: None,
            cpfd: None,
            db_size: 0,
            orig_db_size: 0,
            ckpt_size: 0,
            ckpt_j_size: 0,
            n_extra: option.n_extra,
            n_page: 0,
            n_ref: 0,
            mx_page: option.get_mx_path(),
            n_hit: 0,
            n_miss: 0,
            n_ovfl: 0,
            journal_open: true,
            ckpt_in_use: false,
            ckpt_open: false,
            no_sync: false,
            stats: PageLockState::UNLOCK,
            err_mask: 0,
            temp_file: option.is_temp(),
            read_only: option.read_only,
            need_sync: false,
            dirty_file: false,
            a_in_ckpt: None,
            a_in_journal: None,
            p_first: None,
            p_last: None,
            p_all: None,
            a_hash: HashMap::new(),
        };
        Ok(Arc::new(Mutex::new(pager)))
    }

    /// create page with given page numbers
    /// page numbers should be starts from one
    pub fn get_page(&mut self, pgno: u32) -> Result<Arc<Mutex<PgHdr>>> {
        if pgno == 0 {
            return Err(error_values(SQLExecValue::ERROR));
        }

        todo!()
    }
}
