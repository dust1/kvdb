use std::collections::HashMap;

use super::page::MemPage;
use super::page::PageOne;
use crate::storage::sqlite::page::Pager;

pub struct Btree {
    p_pager: Pager,
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
    pub fn open(z_filename: &str, n_cache: usize) -> Self {
        todo!()
    }
}
