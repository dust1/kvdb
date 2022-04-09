use std::cell::RefCell;
use std::collections::HashMap;
use std::env::temp_dir;
use std::fs::File;
use std::fs::OpenOptions;
use std::path::Path;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;

use crate::error::Error;
use crate::error::Result;

type Pgno = u32;

enum PageLockState {
    UNLOCK,
    READLOCK,
    WRITELOCK,
}

pub struct PagerManager {
    pager: Rc<RefCell<Pager>>,
}

struct Pager {
    fd: RwLock<File>,                      // file descriptor for database
    jfd: File,                             // file descriptor for journal
    cpfd: Option<File>,                    // file descriptor for checkpoint journal
    db_size: i32,                          // number of pages in the database filename
    orig_db_size: u64,                     // db_size before the current change
    ckpt_size: u64,                        // size of database at ckpt_begine()
    ckpt_j_size: u64,                      // size of journal at ckpt_begine()
    n_extra: u64, // add this many bytes to each in-memory page, the user extra data size
    n_page: u32,  // total number of in-memory pages
    n_ref: u32,   // number of in-memory page with PgHdr.n_ref > 0
    mx_page: u32, // max number of pages to hold in cache
    n_hit: u32,   // cache hits
    n_miss: u32,  // cache miss
    n_ovfl: u32,  // LRU overflows
    journal_open: bool, // true if journal file descriptor is valid
    ckpt_open: bool, // true if the checkpoint journal is open
    no_sync: bool, // true if write the database and flush disk
    stats: PageLockState, // the lock state
    err_mask: u8, // one of several kinds of errors, error msg
    temp_file: bool, // true if the z_filename is a temporary file
    read_only: bool, // true if the database readonly
    need_sync: bool, // true if flush disk on the journal before write to the databse
    dirty_file: bool, // true if database file has changed in any way
    p_first: Option<Arc<PgHdr>>, // list of free page
    p_last: Option<Arc<PgHdr>>, // list of free page
    p_all: Option<Arc<PgHdr>>, // list of all pages
    a_hash: Arc<HashMap<u32, Arc<PgHdr>>>, // hash table to map page number of PgHdr
}

struct PgHdr {
    pager: Rc<RefCell<Pager>>,       // the pager
    pgno: Pgno,                      // the page number of this page
    p_next_hash: Option<Arc<PgHdr>>, // hash collection chain for PgHdr.pgno
    p_prev_hash: Option<Arc<PgHdr>>, // hash collection chain for PgHdr.pgno
    n_ref: u32,                      // number of users of this page
    p_next_free: Option<Arc<PgHdr>>, // freelist of pages where n_ref == 0
    p_prev_free: Option<Arc<PgHdr>>, // freelist of pages where n_ref == 0
    p_next_all: Option<Arc<PgHdr>>,  // a list of all pages
    p_prev_all: Option<Arc<PgHdr>>,  // a list of all pages
    in_journal: bool,                // true if has been written to journal
    in_ckpt: bool,                   // true if has been written to the checkpoint journal
    dirty: bool,                     // true if we need write back change
                                     // PAGE_SIZE bytes of page data follow this header
}

impl Pager {
    pub fn open<P: AsRef<Path>>(db_path: Option<P>, max_page: u32, n_extra: u64) -> Result<Self> {
        let mut fd_path = temp_dir().join("kvdb_temp");
        let mut jfd_path = temp_dir().join("kvdb_temp_journal");
        let temp_file = db_path.is_none();

        if let Some(p) = db_path {
            jfd_path = p
                .as_ref()
                .parent()
                .ok_or(Error::Internal("db_path parent unexception".into()))?
                .join("kvdb_journal");
            fd_path = PathBuf::from(p.as_ref());
        }

        let fd = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(fd_path)?;
        let jfd = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(jfd_path)?;
        Ok(Pager {
            fd: RwLock::new(fd),
            jfd,
            cpfd: None,
            db_size: -1,
            orig_db_size: 0,
            ckpt_size: 0,
            ckpt_j_size: 0,
            n_extra,
            n_page: 0,
            n_ref: 0,
            mx_page: if max_page > 5 { max_page } else { 10 },
            n_hit: 0,
            n_miss: 0,
            n_ovfl: 0,
            journal_open: true,
            ckpt_open: false,
            no_sync: false,
            stats: PageLockState::UNLOCK,
            err_mask: 0,
            temp_file,
            read_only: false,
            need_sync: false,
            dirty_file: false,
            p_first: None,
            p_last: None,
            p_all: None,
            a_hash: Arc::new(HashMap::new()),
        })
    }

    /// try to get PgHdr with Pgno, if it was miss in cache, returned None
    pub fn lookup(&mut self, pgno: Pgno) -> Result<Option<PgHdr>> {
        todo!()
    }

    /// read journal and play load it
    fn playback_journal(&mut self) -> Result<()> {
        todo!()
    }
}

impl PgHdr {
    pub fn new(pager: Rc<RefCell<Pager>>, pgno: Pgno) -> Result<Self> {
        Ok(Self {
            pager,
            pgno,
            p_next_hash: None,
            p_prev_hash: None,
            n_ref: 0,
            p_next_free: None,
            p_prev_free: None,
            p_next_all: None,
            p_prev_all: None,
            in_journal: false,
            in_ckpt: false,
            dirty: false,
        })
    }
}

impl PagerManager {
    pub fn new<P: AsRef<Path>>(db_path: Option<P>, mx_page: u32, n_extra: u64) -> Result<Self> {
        let pager = Pager::open(db_path, mx_page, n_extra)?;
        Ok(Self {
            pager: Rc::new(RefCell::new(pager)),
        })
    }

    /// try to get PgHdr with Pgno, if it was miss in cache, we should create it
    pub fn get(&mut self, pgno: Pgno) -> Result<PgHdr> {
        let mut pager = self.pager.as_ref().borrow_mut();
        let mut p_pg = None;
        if pager.n_ref == 0 {
            // first use, try to playback journal
            pager.playback_journal()?;
        } else {
            // try to get page in memory use given pg_no
            p_pg = pager.lookup(pgno)?;
        }

        if p_pg.is_none() {
            pager.n_miss += 1;
            p_pg = Some(PgHdr::new(Rc::clone(&self.pager), pgno)?);
            pager.n_page += 1;
        } else {
            pager.n_hit += 1;
        }

        todo!()
    }
}
