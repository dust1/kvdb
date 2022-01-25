
pub struct Pager {

}

struct PgHdr {
    pager: Box<Pager>,
    page_no: u32,
    p_next_hash: Box<PgHdr>,
    p_prev_hash: Box<PgHdr>,
    n_ref: usize,
    p_next_free: Box<PgHdr>,
    p_prev_free: Box<PgHdr>,
    p_next_all: Box<PgHdr>,
    p_prev_all: Box<PgHdr>,
    in_journal: bool,
    in_ckpt: bool,
    dirty: bool
}