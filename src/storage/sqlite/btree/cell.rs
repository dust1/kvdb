use super::MX_LOCAL_PAYLOAD;

pub struct Cell {
    h: CellHdr,
    a_payload: [u8; MX_LOCAL_PAYLOAD],
    ovfl: u32,
}

pub struct CellHdr {
    left_child: u32,
    n_key: u16,
    i_next: u16,
    n_key_hi: u8,
    n_data_hi: u8,
    n_data: u16,
}

impl Cell {}
