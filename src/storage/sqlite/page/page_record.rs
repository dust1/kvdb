use super::pager::PAGE_SIZE;

pub struct PageRecord {
    pub pgno: u32,
    pub data: [u8; PAGE_SIZE],
}
