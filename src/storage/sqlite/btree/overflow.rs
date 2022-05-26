use std::mem::size_of;

use super::OVERFLOW_SIZE;

pub struct FreeBlk {
    i_size: u16,
    i_next: u16,
}

pub struct OverflowPage {
    i_next: u32,
    a_payload: [u8; OVERFLOW_SIZE],
}

pub struct FreelistInfo {
    n_free: i32,
    a_free: [u32; (OVERFLOW_SIZE - size_of::<i32>()) / size_of::<u32>()],
}
