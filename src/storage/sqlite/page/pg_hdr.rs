use std::sync::Arc;

use derivative::Derivative;
use futures::lock::Mutex;

use super::pager::PAGE_SIZE;
use super::Pager;
use crate::error::Result;

#[derive(Derivative)]
#[derivative(Debug)]

pub struct PgHdr {
    pager: Arc<Mutex<Pager>>,               // the pager
    pgno: u32,                              // the page number of this page
    p_next_hash: Option<Arc<Mutex<PgHdr>>>, // hash collection chain for PgHdr.pgno
    p_prev_hash: Option<Arc<Mutex<PgHdr>>>, // hash collection chain for PgHdr.pgno
    n_ref: u32,                             // number of users of this page
    p_next_free: Option<Arc<Mutex<PgHdr>>>, // freelist of pages where n_ref == 0
    p_prev_free: Option<Arc<Mutex<PgHdr>>>, // freelist of pages where n_ref == 0
    p_next_all: Option<Arc<Mutex<PgHdr>>>,  // a list of all pages
    p_prev_all: Option<Arc<Mutex<PgHdr>>>,  // a list of all pages
    in_journal: bool,                       // true if has been written to journal
    in_ckpt: bool,                          // true if has been written to the checkpoint journal
    dirty: bool,                            // true if we need write back change
    data: [u8; PAGE_SIZE],                  // PAGE_SIZE bytes of page data follow this header
    n_extra: Option<Vec<u8>>,
}

impl PgHdr {
    pub fn set_data(&mut self, data: &[u8]) -> Result<()> {
        self.data.copy_from_slice(data);
        Ok(())
    }
}
