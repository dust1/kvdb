use core::slice;
use std::mem::size_of;
use std::ptr::addr_of;

use kvdb::error::Result;
use kvdb::storage::b_tree::pager::PagerManager;
use serde_derive::Deserialize;
use serde_derive::Serialize;

#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
struct Record {
    pgno: u32,
    data: [u8; 1024],
}

#[test]
fn test_pager() -> Result<()> {
    let mut record = Record {
        pgno: 11,
        data: [0u8; 1024],
    };
    let ptr = addr_of!(record);
    let byte_ptr = ptr as *const u8;
    let record_data = unsafe { slice::from_raw_parts(byte_ptr, size_of::<Record>()) };

    let de_ptr: *const u8 = record_data.as_ptr();
    let record_op: Option<&Record> = unsafe { de_ptr.cast::<Record>().as_ref() };
    if let Some(rd) = record_op {
        assert_eq!(&record, rd)
    }

    Ok(())
}
