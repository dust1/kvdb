use std::collections::HashMap;
use std::mem::size_of;
use std::sync::Arc;
use std::sync::RwLock;

use bincode::deserialize;
use bincode::serialize;
use kvdb::error::Result;
use kvdb::storage::sqlite::btree::page::MemPage;
use kvdb::storage::sqlite::btree::page::MemPageHdr;
use kvdb::storage::sqlite::btree::page::PageHdr;
use kvdb::storage::sqlite::btree::page::PageOne;
use kvdb::storage::sqlite::page::PAGE_SIZE;

#[test]
fn size_of_test() -> Result<()> {
    println!("size of PageOne: {}", size_of::<PageOne>());
    println!("size of MemPage.n: {}", size_of::<MemPageHdr>());
    println!("size of MemPage: {}", size_of::<MemPage>());
    Ok(())
}

#[test]
fn page_one_test() -> Result<()> {
    let data = [0u8; PAGE_SIZE];
    let page1 = unsafe { data.as_ptr().cast::<PageOne>().as_ref() };
    if let Some(p) = page1 {
        println!("PageOne: {:?}", p);
    }
    Ok(())
}

struct DiskData {
    data: [u8; PAGE_SIZE],
}

impl DiskData {
    fn read(&self, start: usize, len: usize) -> &[u8] {
        &self.data[start..start + len]
    }

    fn write(&mut self, data: &[u8], offset: usize) {
        let slice_data = &mut self.data[offset..offset + data.len()];
        slice_data.copy_from_slice(data);
    }
}

#[test]
fn rw_lock_test() -> Result<()> {
    // let mut hash_map = HashMap::new();
    let disk_data = DiskData {
        data: [0u8; PAGE_SIZE],
    };
    let disk_data_obj = Arc::new(RwLock::new(disk_data));

    {
        let read1_lock = Arc::clone(&disk_data_obj);
        let read1 = read1_lock.read()?;
        let data1 = read1.read(0, 10);
        // hash_map.insert(1, read1);
    }

    let write1_lock = Arc::clone(&disk_data_obj);
    let mut write1 = write1_lock.write()?;
    write1.write(&[1u8; 20], 0);

    // let read2_lock = Arc::clone(&disk_data_obj);
    // let read2 = read2_lock.read()?;
    // let data2 = read2.read(10, 10);

    // assert_eq!(data1, data2);

    // drop(hash_map);
    Ok(())
}

#[test]
fn write_test() -> Result<()> {
    let w1 = [1u8; 10];
    let mut w2 = [0u8; 10];

    let w = &mut w2[5..];
    w.copy_from_slice(&w1[..5]);

    let check1 = [0u8, 0, 0, 0, 0, 1, 1, 1, 1, 1];
    assert_eq!(w2, check1);

    Ok(())
}
