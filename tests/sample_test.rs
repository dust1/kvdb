use std::mem::size_of;

use kvdb::storage::sqlite::btree::page::{MemPage, MemPageHdr};


#[test]
fn size_of_test() {
    println!("size of MemPage.n: {}", size_of::<MemPageHdr>());
    println!("size of MemPage: {}", size_of::<MemPage>());
}