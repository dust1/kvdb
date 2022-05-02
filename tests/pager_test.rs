use core::slice;

use std::mem::size_of;
use std::ptr::addr_of;
use std::sync::Arc;
use std::sync::Mutex;

use kvdb::common::options::PagerOption;
use kvdb::error::Result;

use kvdb::storage::sqlite::page::Pager;



#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
struct Record {
    pgno: u32,
    data: [u8; 1024],
}

#[test]
fn pager_get_test() -> Result<()> {
    let pager_option = PagerOption {
        path: None,
        max_page: 10,
        n_extra: 0,
        read_only: false,
    };
    let pager_arc = Arc::new(Mutex::new(Pager::open(pager_option)?));
    let mut pager = pager_arc.as_ref().lock()?;
    let pg_arc = pager.get_page(1, Arc::clone(&pager_arc))?;
    let pg = pg_arc.as_ref().lock()?;
    assert_eq!(pg.get_pgno(), 1);
    let data = [0u8; 1024];
    assert_eq!(pg.get_data(), &data);
    Ok(())
}

#[test]
fn pager_serialize_test() -> Result<()> {
    let record = Record {
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
