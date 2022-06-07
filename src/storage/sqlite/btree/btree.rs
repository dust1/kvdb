use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

use super::page::MemPage;
use super::page::PageOne;
use crate::error::Result;
use crate::storage::Store;
use crate::storage::sqlite::page::Pager;

pub struct Btree {
    p_pager: Arc<Pager>,
    p_cursor: BtCursor,
    page1: PageOne,
    in_trans: u8,
    in_ckpt: u8,
    read_only: u8,
    locks: HashMap<u32, usize>,
}

pub struct BtCursor {
    p_bt: Box<Btree>,
    p_next: Box<BtCursor>,
    p_prev: Box<BtCursor>,
    pgno_root: u32,
    p_page: MemPage,
    idx: i32,
    wr_flag: u8,
    b_skip_next: u8,
    i_match: u8,
}

impl Btree {

    /// open pager and set destructor(maybe)
    pub fn open(z_filename: &str, n_cache: usize) -> Self {
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