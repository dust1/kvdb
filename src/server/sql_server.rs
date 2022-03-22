use log::info;
use tokio::net::TcpListener;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::TcpListenerStream;

use crate::error::{Result, Error};
use crate::storage::Store;

pub struct Server {
    sql_listener: Option<TcpListener>,
}

impl Server {
    pub async fn new(_id: &str, _sql_store: Box<dyn Store>) -> Result<Self> {
        todo!()
    }

    pub async fn listen(mut self, sql_addr: &str) -> Result<Self> {
        let (sql,) = tokio::try_join!(TcpListener::bind(sql_addr),)?;
        info!("Listening on {} (SQL)", sql.local_addr()?);
        self.sql_listener = Some(sql);
        Ok(self)
    }

    pub async fn server(self) -> Result<()> {
        let sql_listener = self.sql_listener.ok_or_else(|| Error::Internal("Must listen before serving".into()));

        todo!()
    }

    /// server sql 
    async fn serve_sql(listener: TcpListener) -> Result<()> {
        let mut listener = TcpListenerStream::new(listener);
        // a client connectioned
        while let Some(socket) = listener.try_next().await? {
            let peer = socket.peer_addr()?;
            // let session = Session
        }
        todo!()
    }

}
