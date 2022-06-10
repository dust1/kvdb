mod btree;
mod cell;
mod overflow;
pub mod page;

use std::mem::size_of;

use self::cell::CellHdr;
use self::page::{PageHdr, MemPage};
use super::page::PAGE_SIZE;

const MIN_CELL_SIZE: usize = size_of::<CellHdr>() + 4;
const MX_CELL: usize = (PAGE_SIZE - size_of::<PageHdr>()) / MIN_CELL_SIZE;
const USABLE_SPACE: usize = PAGE_SIZE - size_of::<PageHdr>();
const MX_LOCAL_PAYLOAD: usize = USABLE_SPACE / 4 - (size_of::<CellHdr>() + size_of::<u32>());
const OVERFLOW_SIZE: usize = PAGE_SIZE - size_of::<u32>();
const Z_MAGIC_HEADER: &str = "** This file contains an SQLite 2.1 database **";
const MAGIC_SIZE: usize = Z_MAGIC_HEADER.len();
const SQLITE_N_BTREE_META: usize = 4;
const EXTRA_SIZE: usize = size_of::<MemPage>() - PAGE_SIZE;
