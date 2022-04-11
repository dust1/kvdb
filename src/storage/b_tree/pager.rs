use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::collections::HashMap;
use std::env::temp_dir;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufWriter;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::mem::size_of;
use std::path::Path;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;

use derivative::Derivative;

use crate::error::Error;
use crate::error::Result;

type Pgno = u32;

const N_PG_HASH: u32 = 2003;

const PAGE_SIZE: usize = 1024;

const JOURNAL_MAGIC: [u8; 8] = [0xca, 0xfe, 0xba, 0xbe, 0xa1, 0xb2, 0xc3, 0xd4];

/// page record in jrounal
#[derive(Clone)]
struct PageRecord {
    pgno: Pgno,
    data: [u8; PAGE_SIZE],
}

#[derive(Debug)]
enum PageLockState {
    UNLOCK,
    READLOCK,
    WRITELOCK,
}

pub struct PagerManager {
    pager: Rc<RefCell<Pager>>,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct Pager {
    fd: Mutex<File>,                   // file descriptor for database
    jfd: Mutex<File>,                  // file descriptor for journal
    cpfd: Option<File>,                // file descriptor for checkpoint journal
    db_size: i32,                      // number of pages in the database filename
    orig_db_size: u64,                 // db_size before the current change
    ckpt_size: u64,                    // size of database at ckpt_begine()
    ckpt_j_size: u64,                  // size of journal at ckpt_begine()
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
    p_first: Option<Rc<RefCell<PgHdr>>>, // list of free page
    p_last: Option<Rc<RefCell<PgHdr>>>, // list of free page
    p_all: Option<Rc<RefCell<PgHdr>>>, // list of all pages
    #[derivative(Debug = "ignore")]
    a_hash: HashMap<u32, Rc<RefCell<PgHdr>>>, // hash table to map page number of PgHdr
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct PgHdr {
    pager: Rc<RefCell<Pager>>,              // the pager
    pgno: Pgno,                             // the page number of this page
    p_next_hash: Option<Arc<PgHdr>>,        // hash collection chain for PgHdr.pgno
    p_prev_hash: Option<Arc<PgHdr>>,        // hash collection chain for PgHdr.pgno
    n_ref: u32,                             // number of users of this page
    p_next_free: Option<Rc<RefCell<PgHdr>>>,        // freelist of pages where n_ref == 0
    p_prev_free: Option<Rc<RefCell<PgHdr>>>,        // freelist of pages where n_ref == 0
    p_next_all: Option<Rc<RefCell<PgHdr>>>, // a list of all pages
    p_prev_all: Option<Rc<RefCell<PgHdr>>>, // a list of all pages
    in_journal: bool,                       // true if has been written to journal
    in_ckpt: bool,                          // true if has been written to the checkpoint journal
    dirty: bool,                            // true if we need write back change
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
            .truncate(true)
            .open(fd_path)?;
        let jfd = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(jfd_path)?;
        Ok(Pager {
            fd: Mutex::new(fd),
            jfd: Mutex::new(jfd),
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
            a_hash: HashMap::new(),
        })
    }

    /// recycle an older page
    pub fn recycle(&mut self) -> Result<()> {
        todo!()
    }

    /// try to get PgHdr with Pgno, if it was miss in cache, returned None
    pub fn lookup(&self, _pgno: Pgno) -> Result<Option<Rc<RefCell<PgHdr>>>> {
        todo!()
    }

    /// playback journal
    /// 1. read page record in journal.
    /// 2. read mx_page in journal
    /// 3. truncates fd file use mx_page * PAGE_SIZE
    /// 4. copy record.data to fd
    fn playback_journal(&mut self) -> Result<()> {
        let mut jfd = self.jfd.lock()?;
        let journal_len = jfd.metadata()?.len();
        if journal_len == 0 {
            return Ok(());
        }
        self.journal_open = true;
        jfd.seek(SeekFrom::Start(0))?;
        // recoreds number
        let mut n_rec = (journal_len as usize - (JOURNAL_MAGIC.len() + size_of::<Pgno>()))
            / size_of::<PageRecord>();
        if n_rec <= 0 {
            return Err(Error::Value(format!(
                "journal file error, size {}",
                journal_len
            )));
        }

        let mut magic_num = [0u8; 8];
        jfd.read_exact(&mut magic_num)?;
        if !magic_num.eq(&JOURNAL_MAGIC) {
            return Err(Error::Value(format!(
                "journal file error, size {}",
                journal_len
            )));
        }
        let mut mx_page_bytes = [0u8; size_of::<Pgno>()];
        jfd.read_exact(&mut mx_page_bytes)?;
        let mx_page = Pgno::from_be_bytes(mx_page_bytes);

        let mut fd = self.fd.lock()?;
        fd.set_len(mx_page as u64 * PAGE_SIZE as u64)?;
        self.db_size = mx_page as i32;

        let mut pg_record_bytes = [0u8; size_of::<PageRecord>()];
        fd.seek(SeekFrom::End(0))?;
        let mut fd_writer = BufWriter::new(&mut *fd);
        loop {
            if n_rec <= 0 {
                break;
            }

            // read
            jfd.read_exact(&mut pg_record_bytes)?;

            let ptr: *const u8 = pg_record_bytes.as_ptr();
            let pr = unsafe {
                ptr.cast::<PageRecord>()
                    .as_ref()
                    .ok_or(Error::Internal("read journal fail".into()))?
            }
            .clone();

            if let Some(_p_pg) = self.lookup(pr.pgno)? {
                // what should i do?
            }

            fd_writer.write_all(&pr.data)?;
            fd_writer.flush()?;
            pg_record_bytes.fill(0);

            n_rec -= 1;
        }

        Ok(())
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
    pub fn new(db_path: Option<&str>, mx_page: u32, n_extra: u64) -> Result<Self> {
        let pager = Pager::open(db_path, mx_page, n_extra)?;
        Ok(Self {
            pager: Rc::new(RefCell::new(pager)),
        })
    }

    /// try to get PgHdr with Pgno, if it was miss in cache, we should create it
    pub fn get(&mut self, pgno: Pgno) -> Result<Rc<RefCell<PgHdr>>> {
        let mut pager = self.pager.as_ref().borrow_mut();
        let mut p_pg = None;
        if pager.n_ref == 0 {
            // first use, try to playback journal
            pager.playback_journal()?;
        } else {
            // try to get page in memory use given pg_no
            p_pg = pager.lookup(pgno)?;
        }

        if let Some(p_pg) = &p_pg {
            page_ref(Rc::clone(p_pg))?;
            pager.n_hit += 1;
        } else {
            // cache miss
            pager.n_miss += 1;
            if pager.n_page >= pager.mx_page {
                pager.recycle()?;
                pager.n_ovfl += 1;
            }

            // create a new page and put it in the head node of of p_all
            let pg_hdr = Rc::new(RefCell::new(PgHdr::new(Rc::clone(&self.pager), pgno)?));

            let mut p_hdr = pg_hdr.as_ref().borrow_mut();
            if let Some(p_all) = &pager.p_all {
                p_hdr.p_next_all = Some(Rc::clone(p_all));

                let mut all = p_all.as_ref().borrow_mut();
                all.p_prev_all = Some(Rc::clone(&pg_hdr));
            }
            pager.p_all = Some(Rc::clone(&pg_hdr));

            pager.n_page += 1;
        }

        p_pg.ok_or(Error::Value(format!("can not get pgno {}", pgno)))
    }


}

/// increment the ref count for a page.
/// if the page is currently on the freelist (the n_ref is zero) then
/// remove it
fn page_ref(pg_hdr: Rc<RefCell<PgHdr>>) -> Result<()> {
    let mut pg_hdr = pg_hdr.as_ref().borrow_mut();
    if pg_hdr.n_ref == 0 {
        if let Some(p_prev_free) = &pg_hdr.p_prev_free {
            let mut p_prev_free = p_prev_free.as_ref().borrow_mut();
            if let Some(p_next_free) = &pg_hdr.p_next_free {
                p_prev_free.p_next_free = Some(Rc::clone(p_next_free));
            } else {
                p_prev_free.p_next_free = None;
            }
        } else {
            let mut pager = pg_hdr.pager.as_ref().borrow_mut();
            if let Some(p_next_free) = &pg_hdr.p_next_free {
                pager.p_first = Some(Rc::clone(p_next_free));
            } else {
                pager.p_first = None;
            }
        }

        if let Some(p_next_free) = &pg_hdr.p_next_free {
            let mut p_next_free = p_next_free.as_ref().borrow_mut();
            if let Some(p_prev_free) = &pg_hdr.p_prev_free {
                p_next_free.p_prev_free = Some(Rc::clone(p_prev_free));
            } else {
                p_next_free.p_prev_free = None;
            }
        } else {
            let mut pager = pg_hdr.pager.as_ref().borrow_mut();
            if let Some(p_prev_free) = &pg_hdr.p_prev_free {
                pager.p_last = Some(Rc::clone(p_prev_free));
            } else {
                pager.p_last = None;
            }
        }

        let mut pager = pg_hdr.pager.as_ref().borrow_mut();
        pager.n_ref += 1;
    }
    pg_hdr.n_ref += 1;
    Ok(())
}

#[inline]
fn pager_hash(pn: u32) -> u32 {
    pn % N_PG_HASH
}
