use std::mem::size_of;
use std::ptr::addr_of;
use std::slice;

use crate::error::Result;

pub fn serialize<'a, T>(t: &'a T) -> Result<&'a [u8]> {
    let ptr = addr_of!(*t);
    let byte_ptr = ptr as *const u8;
    Ok(unsafe { slice::from_raw_parts(byte_ptr, size_of::<T>()) })
}

pub fn deserialize<'a, T>(data: &'a [u8]) -> Result<Option<&'a T>> {
    Ok(unsafe { data.as_ptr().cast::<T>().as_ref() })
}

pub fn deserialize_mut<'a, T>(data: &'a mut [u8]) -> Result<Option<&'a mut T>> {
    Ok(unsafe { data.as_mut_ptr().cast::<T>().as_mut() })
}
