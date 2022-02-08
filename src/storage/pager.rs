use std::{
    fs::File,
    sync::{Arc, RwLock},
};

/// how big to make the hash table used for locating in-memory pages
/// by page number.  Knuth asys this should be a prime number.
pub const N_PG_HASH: usize = 2003;

pub enum PagerState {
    UNLOCK,
    READLOCK,
    WRITELOCK,
}

/// we can use page by this struct
pub struct Pager {
    // name of the database file
    z_filename: &'static str,
    // name of the journal file
    z_journal: &'static str,
    // File descriptors for database
    fd: Arc<RwLock<File>>,
    // File descriptors for journal
    jfd: Arc<RwLock<File>>,
    // File descriptor for the checkpoint journal
    cpfd: Arc<RwLock<File>>,
    // number of pages in the file
    db_size: usize,
    // dbSize before the current change
    orig_db_size: usize,
    // Size of database at ckpt_begin()
    ckpt_size: usize,
    // Size of journal at ckpt_begin()
    ckpt_js_size: usize,
    // Add this many bytes to each in-memory page
    n_extra: usize,
    // Total number of in-memory pages
    n_page: usize,
    // Number of in-memory pages with PgHdr.nRef>0
    n_ref: usize,
    // Maximum number of pages to hold in cache
    mx_page: usize,
    // cache hits
    n_hit: usize,
    // cache miss
    n_miss: usize,
    // LRU overflows
    n_ovfl: usize,
    // True if journal file descriptors is valid
    journal_open: bool,
    // True if the checkpoint journal is open
    ckpt_open: bool,
    // True we are in a checkpoint
    ckpt_in_use: bool,
    // Do not sync the journal if true
    no_sync: bool,
    // SQLITE_UNLOCK, _READLOCK or _WRITELOCK
    state: PagerState,
    // One of several kinds of errors
    err_mask: u8,
    // zFilename is a temporary file
    temp_file: bool,
    // True for a read-only database
    read_only: bool,
    // True if an fsync() is needed on the journal
    need_sync: bool,
    // True if database file has changed in any way
    dirty_file: bool,
    // One bit for each page in the database file?
    a_in_journal: Arc<RwLock<Vec<u8>>>,
    // One bit for each page in the database?
    a_in_ckpt: Arc<RwLock<Vec<u8>>>,
    // List of free pages
    p_first: Arc<RwLock<PgHdr>>,
    p_last: Arc<RwLock<PgHdr>>,
    // List of all pages
    p_all: Arc<RwLock<PgHdr>>,
    // Hash table to map page number of PgHdr
    a_hash: [Arc<RwLock<PgHdr>>; N_PG_HASH],
}

/// Each in-memory image of a page begins with the following header.
/// This header is only visible to this pager module.  
/// The client code that calls pager sees only the data that follows the header.
struct PgHdr {
    // The pager to which this page belongs
    pager: Arc<RwLock<PgHdr>>,
    // the page number for this page
    page_no: u32,
    //  Hash collision chain for PgHdr.pgno
    // |PgHdr| <-> |PgHdr| <-> |PgHdr|
    //    p_prev_hash | p_next_hash
    //             |PgHdr|
    p_next_hash: Option<Arc<RwLock<PgHdr>>>,
    p_prev_hash: Option<Arc<RwLock<PgHdr>>>,
    // number of users of this page
    n_ref: usize,
    // freelist of pages where n_ref==0
    p_next_free: Option<Arc<RwLock<PgHdr>>>,
    p_prev_free: Option<Arc<RwLock<PgHdr>>>,
    // a list of all page
    p_next_all: Option<Arc<RwLock<PgHdr>>>,
    p_prev_all: Option<Arc<RwLock<PgHdr>>>,
    // TRUE if has been written to journal
    in_journal: bool,
    // TRUE if written to the checkpoint
    in_ckpt: bool,
    // TRUE if data change of this page, we need write back changes
    dirty: bool,
}
