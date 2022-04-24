use kvdb::error::Result;
use kvdb::storage::b_tree::pager::PagerManager;

#[test]
fn test_pager() -> Result<()> {
    let mut pager = PagerManager::new(None, 10, 0)?;
    let pg_hdr_1_rc = pager.get(1)?;
    let mut pg_hdr_1 = pg_hdr_1_rc.as_ref().borrow_mut();
    pg_hdr_1.write("Page One".as_bytes(), 0)?;
    pg_hdr_1.commit()?;
    Ok(())
}
