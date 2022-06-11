use core::slice;
use std::mem::size_of;
use std::ptr::addr_of;
use std::sync::Arc;
use std::sync::Mutex;

use bincode::serialize;
use kvdb::common;
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
fn page_test() -> Result<()> {
    let pager_option = PagerOption {
        path: None,
        max_page: 10,
        n_extra: 0,
        read_only: false,
    };
    let pager_arc = Arc::new(Mutex::new(Pager::open(pager_option)?));
    {
        // step 1: open new file and create three page
        let mut pager = pager_arc.as_ref().lock()?;
        pager.get_page(1, Arc::clone(&pager_arc))?;
        pager.get_page(2, Arc::clone(&pager_arc))?;
        pager.get_page(3, Arc::clone(&pager_arc))?;
    }
    {
        // step 2: write data into three different pages and save it on the file
        if let Some(pghdr) = {
            let pager = pager_arc.as_ref().lock()?;
            pager.lookup(1)?
        } {
            let mut pg = pghdr.as_ref().lock()?;
            pg.write("Page One".as_bytes(), 0)?;
        }
        let mut pager = pager_arc.as_ref().lock()?;
        pager.commit()?;
    }
    {
        if let Some(pghdr) = {
            let pager = pager_arc.as_ref().lock()?;
            pager.lookup(2)?
        } {
            let mut pg = pghdr.as_ref().lock()?;
            pg.write("Page Two".as_bytes(), 0)?;
        }

        if let Some(pghdr) = {
            let pager = pager_arc.as_ref().lock()?;
            pager.lookup(3)?
        } {
            let mut pg = pghdr.as_ref().lock()?;
            pg.write("Page Three".as_bytes(), 0)?;
        }
        let mut pager = pager_arc.as_ref().lock()?;
        pager.commit()?;
    }
    {
        // step 3: read pages to make sure changes commited

        if let Some(pghdr) = {
            let pager = pager_arc.as_ref().lock()?;
            pager.lookup(1)?
        } {
            let pg = pghdr.as_ref().lock()?;
            let data = &pg.get_data()[..8];
            assert_eq!(data, "Page One".as_bytes());
        }
        if let Some(pghdr) = {
            let pager = pager_arc.as_ref().lock()?;
            pager.lookup(2)?
        } {
            let pg = pghdr.as_ref().lock()?;
            let data = &pg.get_data()[..8];
            assert_eq!(data, "Page Two".as_bytes());
        }
        if let Some(pghdr) = {
            let pager = pager_arc.as_ref().lock()?;
            pager.lookup(3)?
        } {
            let pg = pghdr.as_ref().lock()?;
            let data = &pg.get_data()[..10];
            assert_eq!(data, "Page Three".as_bytes());
        }
    }
    {
        // step 4: write data into the third page and before commit the changes, rollback to the previous state
        if let Some(pghdr) = {
            let pager = pager_arc.as_ref().lock()?;
            pager.lookup(3)?
        } {
            let mut pg = pghdr.as_ref().lock()?;
            pg.write("Page test rollback".as_bytes(), 0)?;
        }
        {
            let mut pager = pager_arc.as_ref().lock()?;
            pager.rollback()?;
            pager.commit()?;
        }
        let pghdr = {
            let mut pager = pager_arc.as_ref().lock()?;
            pager.get_page(3, Arc::clone(&pager_arc))?
        };

        let pg = pghdr.as_ref().lock()?;
        let data = &pg.get_data()[..10];
        assert_eq!(data, "Page Three".as_bytes());
    }
    let mut pager = pager_arc.as_ref().lock()?;
    assert_eq!(pager.pagecount()?, 3);

    Ok(())
}

#[test]
fn pager_serialize_test() -> Result<()> {
    let record = Record {
        pgno: 11,
        data: [0u8; 1024],
    };
    let record_data = common::ptr_util::serialize(&record)?;
    let record_op: Option<&Record> = common::ptr_util::deserialize(record_data)?;
    if let Some(rd) = record_op {
        assert_eq!(&record, rd)
    }

    Ok(())
}
