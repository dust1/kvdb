use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::mem::size_of;
use std::os::unix::prelude::FileExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::sync::RwLockWriteGuard;

use derivative::Derivative;

use super::page_error::error_values;
use super::page_error::SQLExecValue;
use super::page_record::PageRecord;
use super::PgHdr;
use crate::common::options::PagerOption;
use crate::error::Error;
use crate::error::Result;

/// page size
pub(super) const PAGE_SIZE: usize = 1024;

/// How big to make the hash table used for locating in-memory pages
/// by page number.
const PG_HASH: usize = 2003;

/// jfd magic number
pub(super) const JOURNAL_MAGIC: [u8; 8] = [0xca, 0xfe, 0xba, 0xbe, 0xa1, 0xb2, 0xc3, 0xd4];

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
    fd: RwLock<File>,
    // file descriptor for journal
    jfd: RwLock<File>,
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
    err_mask: SQLExecValue,
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
    pub fn open(option: PagerOption) -> Result<Self> {
        let (z_filename, z_journal) = option.get_paths()?;
        let fd = RwLock::new(File::create(z_filename.as_path())?);
        let jfd = RwLock::new(File::create(z_journal.as_path())?);
        let pager = Pager {
            z_filename,
            z_journal,
            fd,
            jfd,
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
            err_mask: SQLExecValue::OK,
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
        Ok(pager)
    }

    pub fn add_ref(&mut self) {
        self.n_ref += 1;
    }

    pub fn set_last(&mut self, last: Option<Arc<Mutex<PgHdr>>>) {
        self.p_last = last;
    }

    pub fn set_first(&mut self, first: Option<Arc<Mutex<PgHdr>>>) {
        self.p_first = first;
    }

    /// create page with given page numbers
    /// page numbers should be starts from one
    pub fn get_page(&mut self, pgno: u32, pg_ref: Arc<Mutex<Pager>>) -> Result<Arc<Mutex<PgHdr>>> {
        if pgno == 0 {
            return Err(error_values(SQLExecValue::ERROR));
        }
        if self.err_mask != SQLExecValue::OK {
            return Err(error_values(self.err_mask));
        }

        let mut p_pg = None;

        if self.n_ref == 0 {
            // this is a new pager or not used
            let read_lock = match self.fd.read() {
                Err(_) => return Err(error_values(SQLExecValue::BUSY)),
                Ok(rl) => {
                    self.stats = PageLockState::READLOCK;
                    rl
                }
            };

            if self.z_journal.exists() && read_lock.metadata()?.len() > 0 {
                drop(read_lock);
                let mut write_fd = match self.fd.write() {
                    Err(_) => {
                        return Err(error_values(SQLExecValue::BUSY));
                    }
                    Ok(wl) => {
                        self.stats = PageLockState::WRITELOCK;
                        wl
                    }
                };
                self.journal_open = true;
                let mut read_jfd = self.jfd.read()?;
                let db_size = pager_playback(self, &mut write_fd, &mut read_jfd)?;
                self.db_size = db_size;
                drop(write_fd);
            }
        } else {
            p_pg = self.lookup(pgno)?;
        }

        if let Some(p_pg) = p_pg.as_ref() {
            let mut pg = p_pg.lock()?;
            pg.page_ref()?;
        } else {
            // cache miss
            self.n_miss += 1;
            if self.n_page < self.mx_page || self.p_first.is_none() {
                // create a new page
                let pg_hdr = match PgHdr::new(pg_ref, pgno) {
                    Ok(p) => p,
                    Err(_) => {
                        self.unwritelock()?;
                        self.err_mask = SQLExecValue::NOMEM;
                        return Err(error_values(SQLExecValue::BUSY));
                    }
                };
                let pg_arc = Arc::new(Mutex::new(pg_hdr));

                // put it in the head node of p_all
                let p_pg_ref = Arc::clone(&pg_arc);
                p_pg = Some(Arc::clone(&pg_arc));

                let mut p_hdr = p_pg_ref.as_ref().lock()?;
                if let Some(p_all) = self.p_all.as_ref() {
                    // if has other pgHdr
                    p_hdr.set_next_all(Arc::clone(p_all));

                    let mut prev_head = p_all.lock()?;
                    prev_head.set_prev_all(Arc::clone(&pg_arc));
                }

                self.p_all = Some(Arc::clone(&pg_arc));
                self.n_page += 1;
            } else {
                // recycle pages
                let mut p_first = self.p_first.as_ref().map(Arc::clone);
                while let Some(free_node) = p_first.as_ref().map(Arc::clone) {
                    let node = free_node.as_ref().lock()?;
                    // try to find a free page that is not being used
                    if node.is_dirty() {
                        p_first = node.get_next_free();
                    } else {
                        break;
                    }
                }

                if p_first.is_none() {
                    // all page was dirty, sync all page
                    match self.sync_all_pages() {
                        Ok(_) => {}
                        Err(_) => {
                            self.rollback()?;
                            return Err(error_values(SQLExecValue::IOERR));
                        }
                    }

                    p_first = self.p_first.as_ref().map(Arc::clone);
                }

                // remove p_first in free list
                if let Some(free_first) = p_first {
                    let mut free_node = free_first.as_ref().lock()?;

                    // remove it with free list
                    if let Some(free_prev) = free_node.get_prev_free() {
                        let mut prev_node = free_prev.as_ref().lock()?;
                        prev_node.set_next_free(free_node.get_next_free());
                    } else {
                        self.p_first = free_node.get_next_free();
                    }
                    if let Some(free_next) = free_node.get_next_free() {
                        let mut next_node = free_next.as_ref().lock()?;
                        next_node.set_prev_free(free_node.get_prev_free());
                    } else {
                        self.p_last = free_node.get_prev_free();
                    }
                    free_node.set_next_free(None);
                    free_node.set_prev_free(None);

                    // remove it with hash list
                    if let Some(hash_next) = free_node.get_next_hash() {
                        let mut next_node = hash_next.as_ref().lock()?;
                        next_node.set_prev_hash(free_node.get_prev_hash());
                    }
                    if let Some(hash_prev) = free_node.get_prev_hash() {
                        let mut prev_node = hash_prev.as_ref().lock()?;
                        prev_node.set_next_hash(free_node.get_next_hash());
                    } else {
                        // in the hash list head
                        let hash_key = pgno_hash(free_node.get_pgno());
                        match free_node.get_next_hash() {
                            Some(node) => self.a_hash.insert(hash_key, node),
                            None => self.a_hash.remove(&hash_key),
                        };
                    }
                    free_node.set_next_hash(None);
                    free_node.set_prev_hash(None);

                    drop(free_node);
                    p_pg = Some(free_first);
                } else {
                    return Err(error_values(SQLExecValue::IOERR));
                }
                self.n_ovfl += 1;
            }

            if let Some(pg_hdr) = p_pg.as_ref() {
                let mut pg = pg_hdr.lock()?;

                match self.a_in_journal.as_ref() {
                    Some(in_journal) if pgno < self.orig_db_size => {
                        if let Some(index) = in_journal.get(pgno as usize / 8) {
                            pg.set_journal((*index & (1 << (pgno % 7))) != 0);
                        }
                    }
                    _ => { /* do not anything */ }
                }

                match self.a_in_ckpt.as_ref() {
                    Some(in_ckpt) if (pgno as u64) < self.ckpt_size => {
                        if let Some(index) = in_ckpt.get(pgno as usize / 8) {
                            pg.set_ckpt((*index & (1 << (pgno % 7))) != 0);
                        }
                    }
                    _ => { /* do not anything */ }
                }

                pg.pg_ref();
                self.n_ref += 1;

                let hash_key = pgno_hash(pgno);
                pg.set_next_hash(self.a_hash.get(&hash_key).map(Arc::clone));
                self.a_hash.insert(hash_key, Arc::clone(pg_hdr));
                if let Some(next_hash) = pg.get_next_hash() {
                    let mut next = next_hash.as_ref().lock()?;
                    next.set_prev_hash(Some(Arc::clone(pg_hdr)));
                }

                if self.db_size == 0 {
                    self.pagecount()?;
                }

                if self.db_size >= pgno {
                    let fd_read = self.fd.read()?;
                    pg.read_data(fd_read)?;
                }
            }
        }

        p_pg.ok_or_else(|| error_values(SQLExecValue::IOERR))
    }

    fn pagecount(&mut self) -> Result<()> {
        if self.db_size != 0 {
            return Ok(());
        }
        let db_len = self.fd.read()?.metadata()?.len();
        if self.stats != PageLockState::UNLOCK {
            self.db_size = (db_len / PAGE_SIZE as u64) as u32;
        }
        Ok(())
    }

    /// Rollback all changes.
    fn rollback(&mut self) -> Result<()> {
        todo!()
    }

    /// Sync the journal and then write all free dirty pages to the database file.
    fn sync_all_pages(&mut self) -> Result<()> {
        todo!()
    }

    /// when this routine is called. the pager has the journal file open and
    /// a write lock on the database.
    /// this routine release the database write lock and acquires a read lock
    /// in its place.
    /// The journal file is deleted and closed
    fn unwritelock(&mut self) -> Result<()> {
        todo!()
    }

    /// find a page in the hash table given its page number.
    pub fn lookup(&self, pgno: u32) -> Result<Option<Arc<Mutex<PgHdr>>>> {
        if pgno == 0 {
            return Ok(None);
        }

        if let Some(hash_list) = self.a_hash.get(&pgno_hash(pgno)) {
            let mut hash_node = Some(Arc::clone(hash_list));
            while let Some(node) = hash_node.as_ref() {
                let hash_n = Arc::clone(node);
                let n = hash_n.as_ref().lock()?;
                if n.get_pgno().eq(&pgno) {
                    break;
                }
                hash_node = n.get_next_hash();
            }
            return Ok(hash_node);
        }
        Ok(None)
    }
}

fn pager_playback(
    pager: &Pager,
    fd: &mut RwLockWriteGuard<File>,
    jfd: &mut RwLockReadGuard<File>,
) -> Result<u32> {
    let mut n_rec = match jfd.metadata() {
        Err(_) => return Err(error_values(SQLExecValue::CORRUPT)),
        Ok(metadata) => metadata.len(),
    };

    n_rec = (n_rec - JOURNAL_MAGIC.len() as u64 - size_of::<u32>() as u64)
        / size_of::<PageRecord>() as u64;
    if n_rec == 0 {
        return Err(error_values(SQLExecValue::CORRUPT));
    }

    let mut magic = [0u8; JOURNAL_MAGIC.len()];
    match jfd.read_exact_at(&mut magic, 0) {
        Err(_) => return Err(error_values(SQLExecValue::CORRUPT)),
        Ok(_) => {}
    };

    let mut mx_pg_data = [0u8; size_of::<u32>()];
    match jfd.read_exact_at(&mut mx_pg_data, JOURNAL_MAGIC.len() as u64) {
        Err(_) => return Err(error_values(SQLExecValue::CORRUPT)),
        Ok(_) => {}
    };
    let mx_pg = u32::from_be_bytes(mx_pg_data);

    match fd.set_len(mx_pg as u64 * PAGE_SIZE as u64) {
        Err(_) => return Err(error_values(SQLExecValue::CORRUPT)),
        Ok(_) => {}
    }

    for pgno in 1..n_rec + 1 {
        pager_playback_one_page(pgno as u32, pager, fd, jfd)?;
    }

    Ok(mx_pg)
}

/// read a single page from the journal file opened file descripter jfd.
/// playback this one page.
fn pager_playback_one_page(
    pgno: u32,
    pager: &Pager,
    fd: &mut RwLockWriteGuard<File>,
    jfd: &mut RwLockReadGuard<File>,
) -> Result<()> {
    const RECORD_SIZE: usize = size_of::<PageRecord>();
    let mut record_data = [0u8; RECORD_SIZE];
    let offset = JOURNAL_MAGIC.len() + size_of::<u32>() + RECORD_SIZE * (pgno - 1) as usize;
    jfd.read_exact_at(&mut record_data, offset as u64)?;

    let byte_ptr: *const u8 = record_data.as_ptr();
    let record = unsafe {
        byte_ptr
            .cast::<PageRecord>()
            .as_ref()
            .ok_or_else(|| Error::Internal("read journal fail".into()))?
    };

    if let Some(pghdr) = pager.lookup(pgno)? {
        let mut pg = pghdr.as_ref().lock()?;
        pg.set_data(&record.data);
    }

    fd.seek(SeekFrom::Start((record.pgno - 1) as u64 * PAGE_SIZE as u64))?;
    fd.write_all(&record.data)?;

    Ok(())
}

fn pgno_hash(pgno: u32) -> u32 {
    pgno % PG_HASH as u32
}
