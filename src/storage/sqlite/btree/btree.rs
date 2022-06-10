use std::collections::HashMap;
use std::fmt::Display;
use std::mem::size_of;
use std::sync::Arc;
use std::sync::Mutex;

use super::page::MemPage;
use super::page::PageOne;
use crate::common::options::PagerOption;
use crate::error::Result;
use crate::storage::sqlite::page::PAGE_SIZE;
use crate::storage::sqlite::page::Pager;
use crate::storage::Store;

pub struct Btree {
    pager: Arc<Mutex<Pager>>,
    cursor: Option<BtCursor>,
    page1: Option<PageOne>,
    in_trans: u8,
    in_ckpt: u8,
    read_only: bool,
    locks: HashMap<u32, usize>,
    // btree table id
    table_id: u32,
}

pub struct BtCursor {
    p_bt: Arc<Btree>,
    p_next: Arc<BtCursor>,
    p_prev: Arc<BtCursor>,
    pgno_root: u32,
    p_page: MemPage,
    idx: i32,
    wr_flag: u8,
    b_skip_next: u8,
    i_match: u8,
}

impl Btree {
    /// open pager and set destructor(maybe)
    pub fn open(filename: &'static str, n_cache: usize) -> Result<Btree> {
        let pager_option = PagerOption {
            path: Some(filename),
            max_page: if n_cache < 10 {
                10
            } else {
                n_cache as u32
            },
            n_extra: 0,
            read_only: false,
        };
        let pager = Pager::open(pager_option)?;
        let read_only = pager.read_only();
        let mut btree = Self {
            pager: Arc::new(Mutex::new(pager)),
            cursor: None,
            page1: None,
            in_trans: 0,
            in_ckpt: 0,
            read_only,
            locks: HashMap::new(),
            table_id: 0,
        };

        // create a table
        btree.btree_begin_trans()?;
        btree.btree_create_table()?;
        btree.btree_commit_trans()?;
        Ok(btree)
    }

    /// create new table and return table id
    pub fn btree_create_table(&mut self) -> Result<u32> {
        todo!()
    }

    /// begin a btree transaction.
    /// it is different from database and page transaction.
    pub fn btree_begin_trans(&mut self) -> Result<()> {
        todo!()
    }

    /// commit btree transaction
    pub fn btree_commit_trans(&mut self) -> Result<()> {
        todo!()
    }

    /// rollback btree transaction
    pub fn btree_rollback_trans(&mut self) -> Result<()> {
        todo!()
    }
}

impl Display for Btree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Store for Btree {
    fn delete(&mut self, key: &[u8]) -> crate::error::Result<()> {
        todo!()
    }

    fn flush(&mut self) -> crate::error::Result<()> {
        todo!()
    }

    fn get(&self, key: &[u8]) -> crate::error::Result<Option<Vec<u8>>> {
        todo!()
    }

    fn scan(&self, range: crate::common::range::Range) -> crate::common::range::Scan {
        todo!()
    }

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> crate::error::Result<()> {
        todo!()
    }
}
