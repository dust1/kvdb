use derivative::Derivative;

use super::PAGE_SIZE;
use crate::common;
use crate::error::Result;
use crate::storage::sqlite::btree::page::PageOne;

/// a separate disk data objct
#[derive(Derivative)]
#[derivative(Debug)]
pub struct DiskData {
    data: [u8; PAGE_SIZE],
    is_dirty: bool,
}

impl DiskData {
    pub fn new() -> DiskData {
        Self {
            data: [0u8; PAGE_SIZE],
            is_dirty: false,
        }
    }

    pub fn to_page1(&self) -> Result<Option<&PageOne>> {
        common::ptr_util::deserialize(&self.data)
    }

    pub fn to_page1_mut(&mut self) -> Result<Option<&mut PageOne>> {
        common::ptr_util::deserialize_mut(&mut self.data)
    }

    pub fn read(&self, buf: &mut [u8], offset: usize) -> Result<usize> {
        if offset >= self.data.len() {
            return Ok(0);
        }
        let end = if buf.len() + offset > self.data.len() {
            self.data.len()
        } else {
            buf.len() + offset
        };
        buf.copy_from_slice(&self.data[offset..end]);
        Ok(end - offset)
    }

    pub fn write(&mut self, buf: &[u8], offset: usize) -> Result<usize> {
        if offset >= self.data.len() {
            return Ok(0);
        }
        let len = if buf.len() + offset > self.data.len() {
            self.data.len() - offset
        } else {
            buf.len()
        };

        if len == 0 {
            return Ok(0);
        }

        let write_slice = &mut self.data[offset..offset + len];
        write_slice.copy_from_slice(&buf[..len]);
        self.is_dirty = true;
        Ok(len)
    }
}
