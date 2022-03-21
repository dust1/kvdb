use tokio::net::TcpListener;

use crate::error::Result;
use crate::storage::Store;

pub struct Server {
    sql_listener: Option<TcpListener>,
}

impl Server {
    pub async fn new(_id: &str, _sql_store: Box<dyn Store>) -> Result<Self> {
        todo!()
    }

    pub async fn listen(self, _sql_addr: &str) -> Result<Self> {
        todo!()
    }

    pub async fn server(self) -> Result<()> {
        todo!()
    }
}
