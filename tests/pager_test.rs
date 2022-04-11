use kvdb::{error::Result, storage::b_tree::pager::PagerManager};



#[test]
fn test_pager() -> Result<()> {
    let mut pager_manager = PagerManager::new(None, 10, 0)?;
    let pg = pager_manager.get(1)?;
    let p = pg.borrow();
    println!("{:?}", p);
    Ok(())
}