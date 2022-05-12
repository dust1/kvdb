use std::fs::File;
use std::os::unix::prelude::FileExt;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLockReadGuard;

use derivative::Derivative;

use super::page_error::error_values;
use super::page_error::SQLExecValue;
use super::pager::PAGE_SIZE;
use super::Pager;
use crate::error::Result;
use crate::storage::sqlite::page::pager::PageLockState;

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
    dirty: bool,                            // true if we need write back change
    data: [u8; PAGE_SIZE],                  // PAGE_SIZE bytes of page data follow this header
}

impl PgHdr {
    pub fn new(pager: Arc<Mutex<Pager>>, pgno: u32) -> Result<PgHdr> {
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
            dirty: false,
            data: [0u8; PAGE_SIZE],
        })
    }

    /// write data to page
    pub fn write(&mut self, data: &[u8], offset: u64) -> Result<()> {
        if PAGE_SIZE < offset as usize || offset as usize + data.len() > PAGE_SIZE {
            return Err(error_values(SQLExecValue::NOMEM));
        }
        self.write_begin()?;
        let data_slice = &mut self.data[offset as usize..offset as usize + data.len()];
        data_slice.copy_from_slice(data);
        Ok(())
    }

    /// mark a data page as writeable. the page is written into the journal
    /// if it is not there already.
    ///
    /// My:
    ///     this is to check wheter the page writes the original data
    ///     to the journal file and checkpoint file before writing the data
    fn write_begin(&mut self) -> Result<()> {
        let mut pager = self.pager.as_ref().lock()?;
        pager.err_mask()?;
        if pager.read_only() {
            return Err(error_values(SQLExecValue::PERM));
        }
        self.dirty = true;
        if self.in_journal {
            pager.set_dirty_file(true);
            return Ok(());
        }

        assert_ne!(pager.get_state(), PageLockState::UNLOCK);

        pager.page_begin()?;
        pager.set_dirty_file(true);
        assert_eq!(pager.get_state(), PageLockState::WRITELOCK);
        assert!(pager.get_journal_open());

        if !self.in_journal && self.pgno <= pager.get_orig_db_size() {
            // the page not writed to journal, and this not a new page
            // the transaction journal now exists and we have a write lock on
            // the main database file.
            // write the current page to the transaction journal
            // if it is not there already.
            match pager.write_journal(self.pgno, &self.data) {
                Ok(_) => {}
                Err(_) => {
                    pager.rollback()?;
                    pager.put_err_mask(SQLExecValue::FULL);
                    return Err(error_values(SQLExecValue::FULL));
                }
            }
            assert!(pager.get_a_journal().is_some());
            pager.put_a_journal(self.pgno);
            let no_sync = pager.get_no_sync();
            pager.set_need_sync(!no_sync);
            self.in_journal = true;
        }

        if pager.get_db_size() < self.pgno {
            pager.set_db_size(self.pgno);
        }

        Ok(())
    }

    /// Increment the reference count for a page.
    pub fn page_ref(&mut self) -> Result<()> {
        if self.n_ref == 0 {
            let mut pager = self.pager.as_ref().lock()?;
            if let Some(prev_free) = self.p_prev_free.as_ref() {
                let mut free_node = prev_free.lock()?;
                free_node.set_next_free(self.get_next_free());
            } else {
                pager.set_first(self.get_next_free());
            }

            if let Some(next_free) = self.p_next_free.as_ref() {
                let mut free_node = next_free.lock()?;
                free_node.set_prev_free(self.get_prev_free());
            } else {
                pager.set_last(self.get_prev_free());
            }

            pager.add_ref();
        }
        self.n_ref += 1;
        Ok(())
    }

    pub fn pg_ref(&mut self) {
        self.n_ref += 1;
    }

    pub fn set_journal(&mut self, journal: bool) {
        self.in_journal = journal;
    }

    pub fn get_ref(&self) -> u32 {
        self.n_ref
    }

    pub fn set_dirty(&mut self, dirty: bool) {
        self.dirty = dirty;
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    pub fn set_next_all(&mut self, next_all: Arc<Mutex<PgHdr>>) {
        self.p_next_all = Some(next_all);
    }

    pub fn set_prev_all(&mut self, prev_all: Arc<Mutex<PgHdr>>) {
        self.p_prev_all = Some(prev_all);
    }

    pub fn read_fd(&mut self, fd: RwLockReadGuard<File>) -> Result<()> {
        match fd.read_exact_at(&mut self.data, (self.pgno - 1) as u64 * PAGE_SIZE as u64) {
            Ok(_) => Ok(()),
            Err(_) => Err(error_values(SQLExecValue::IOERR)),
        }
    }

    pub fn get_data(&self) -> &[u8] {
        &self.data
    }

    pub fn set_data(&mut self, data: &[u8]) {
        self.data.copy_from_slice(data);
    }

    pub fn get_pgno(&self) -> u32 {
        self.pgno
    }

    pub fn set_prev_hash(&mut self, prev_hash: Option<Arc<Mutex<PgHdr>>>) {
        self.p_prev_hash = prev_hash
    }

    pub fn get_prev_hash(&self) -> Option<Arc<Mutex<PgHdr>>> {
        self.p_prev_hash.as_ref().map(Arc::clone)
    }

    pub fn set_next_hash(&mut self, next_hash: Option<Arc<Mutex<PgHdr>>>) {
        self.p_next_hash = next_hash
    }

    pub fn get_next_hash(&self) -> Option<Arc<Mutex<PgHdr>>> {
        self.p_next_hash.as_ref().map(Arc::clone)
    }

    pub fn set_prev_free(&mut self, prev_free: Option<Arc<Mutex<PgHdr>>>) {
        self.p_prev_free = prev_free
    }

    pub fn get_prev_free(&self) -> Option<Arc<Mutex<PgHdr>>> {
        self.p_prev_free.as_ref().map(Arc::clone)
    }

    pub fn set_next_free(&mut self, next_free: Option<Arc<Mutex<PgHdr>>>) {
        self.p_next_free = next_free;
    }

    pub fn get_next_free(&self) -> Option<Arc<Mutex<PgHdr>>> {
        self.p_next_free.as_ref().map(Arc::clone)
    }

    pub fn get_next_all(&self) -> Option<Arc<Mutex<PgHdr>>> {
        self.p_next_all.as_ref().map(Arc::clone)
    }
}
