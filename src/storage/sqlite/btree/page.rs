use std::sync::Arc;

use super::cell::Cell;
use super::MAGIC_SIZE;
use super::MX_CELL;
use super::SQLITE_N_BTREE_META;
use crate::storage::sqlite::page::PAGE_SIZE;

/// the first page of the database file contains a magic header string to identify the file
/// as an SQLITE database file.
pub struct PageOne {
    // String that identifies the file as a database
    z_magic: [u8; MAGIC_SIZE],
    // Integer to verify correct byte order
    i_magic: i32,
    // a pointer to the first free page of the file.
    // BY ME: how the save a pointer?
    free_list: u32,
    // Integer to verify correct byte order
    n_free: i32,
    a_meta: [u32; SQLITE_N_BTREE_META - 1],
}

/// each database page has a header that is an instance of this structure.
///
pub struct PageHdr {
    rigth_child: u32,
    first_cell: u16,
    // is 0 if there is no free space on this page.
    // otherwise, first_free is index in MemPage.u.a_disk[]
    first_free: u16,
}

#[repr(C)]
pub enum MemPageHdr {
    DISK([u8;PAGE_SIZE]),
    HDR(PageHdr)
}

#[repr(C)]
pub struct MemPage {
    n: MemPageHdr,
    is_init: i32,
    p_parent: Arc<MemPage>,
    n_free: i32,
    n_cell: i32,
    is_overfull: i32,
    ap_cell: Arc<[Cell; MX_CELL + 2]>,
}
