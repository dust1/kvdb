use std::sync::Arc;

use super::cell::Cell;
use super::MAGIC_SIZE;
use super::MX_CELL;
use super::SQLITE_N_BTREE_META;
use crate::error::Error;
use crate::error::Result;
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
#[derive(Copy)]
pub struct PageHdr {
    pub rigth_child: u32,
    pub first_cell: u16,
    // is 0 if there is no free space on this page.
    // otherwise, first_free is index in MemPage.u.a_disk[]
    pub first_free: u16,
}

impl Clone for PageHdr {
    fn clone(&self) -> Self {
        Self {
            rigth_child: self.rigth_child.clone(),
            first_cell: self.first_cell.clone(),
            first_free: self.first_free.clone(),
        }
    }
}

#[repr(C)]
pub union MemPageHdr {
    pub disk: [u8; PAGE_SIZE],
    pub hdr: PageHdr,
}

pub struct MemPage {
    n: MemPageHdr,
    is_init: i32,
    p_parent: Option<Arc<MemPage>>,
    n_free: i32,
    n_cell: i32,
    is_overfull: i32,
    ap_cell: [usize; MX_CELL + 2],
}

impl MemPage {

    pub fn from_data(data: &[u8]) -> Result<MemPage> {
        if data.len() != PAGE_SIZE {
            return Err(Error::Serialization);
        }
        // FIXME: this means that the data in MemPage and PageHdr is 
        // no longer the same piece of memory
        let mut disk_data = [0u8; PAGE_SIZE];
        disk_data.copy_from_slice(data);

        Ok(Self {
            n: MemPageHdr {
                disk: disk_data,
            },
            is_init: 1,
            p_parent: None,
            n_free: 1,
            n_cell: 2,
            is_overfull: 11,
            ap_cell: [0; MX_CELL + 2],
        })
    }

}
