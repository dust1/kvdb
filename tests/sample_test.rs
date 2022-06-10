use std::mem::size_of;

use bincode::{deserialize, serialize};
use kvdb::{storage::sqlite::{btree::page::{MemPage, MemPageHdr, PageHdr}, page::PAGE_SIZE}, error::Result};


#[test]
fn size_of_test() -> Result<()> {
    println!("size of MemPage.n: {}", size_of::<MemPageHdr>());
    println!("size of MemPage: {}", size_of::<MemPage>());
    Ok(())
}