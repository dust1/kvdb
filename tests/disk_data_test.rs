use std::sync::Arc;
use std::sync::RwLock;

use kvdb::common;
use kvdb::error::Result;
use kvdb::storage::sqlite::btree::page::PageOne;
use kvdb::storage::sqlite::page::DiskData;
use kvdb::storage::sqlite::page::PAGE_SIZE;

#[test]
fn disk_data_test() -> Result<()> {
    let mut disk_data = DiskData::new();
    let write_data = [19u8; 10];
    let write_len = disk_data.write(&write_data, 10)?;
    assert_eq!(write_len, write_data.len());

    let mut read_data = [0u8; 10];
    let read_len = disk_data.read(&mut read_data, 15)?;
    assert_eq!(read_len, read_data.len());

    let check_read = [19u8, 19, 19, 19, 19, 0, 0, 0, 0, 0];
    assert_eq!(check_read, read_data);

    Ok(())
}

#[test]
fn disk_data_page1_test() -> Result<()> {
    let disk_data_lock = Arc::new(RwLock::new(DiskData::new()));
    let mut disk_data = disk_data_lock.write()?;
    if let Some(page1) = disk_data.to_page1_mut()? {
        page1.set_free_list(23);
    }

    let mut d = [0u8; PAGE_SIZE];
    disk_data.read(&mut d, 0)?;

    if let Some(check) = common::ptr_util::deserialize::<PageOne>(&d)? {
        let free_list = check.get_free_list();
        assert_eq!(free_list, 23);
    } else {
        assert!(false);
    }

    Ok(())
}
