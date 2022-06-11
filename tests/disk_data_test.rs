use kvdb::error::Result;
use kvdb::storage::sqlite::page::DiskData;

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
