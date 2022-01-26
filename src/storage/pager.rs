use std::{fs::File, sync::{Arc, RwLock}};

/// how big to make the hash table used for locating in-memory pages
/// by page number.  Knuth asys this should be a prime number.
pub const N_PG_HASH:usize = 2003;

pub enum PagerState {
    UNLOCK,
    READLOCK,
    WRITELOCK
}

/// we can use page by this struct
pub struct Pager {
    z_filename: &'static str,
    z_journal: &'static str,
    fd: Arc<RwLock<File>>,
    jfd: Arc<RwLock<File>>,
    cpfd: Arc<RwLock<File>>,
    db_size: usize,
    orig_db_size: usize,
    ckpt_size: usize,
    ckpt_js_size: usize,
    n_extra: usize,
    n_page: usize,
    n_ref: usize,
    mx_page: usize,
    n_hit: usize,
    n_miss: usize,
    n_ovfl: usize,
    journal_open: bool,
    ckpt_open: bool,
    ckpt_in_use: bool,
    no_sync: bool,
    state: PagerState,
    err_mask: u8,
    temp_file: bool,
    read_only: bool,
    need_sync: bool,
    dirty_file: bool,
    a_in_journal: Arc<RwLock<Vec<u8>>>,
    a_in_ckpt: Arc<RwLock<Vec<u8>>>,
    p_first: Arc<RwLock<PgHdr>>,
    p_last: Arc<RwLock<PgHdr>>,
    p_all: Arc<RwLock<PgHdr>>,
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
    dirty: bool
}