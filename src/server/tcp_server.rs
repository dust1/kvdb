use log::error;
use log::info;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::StreamExt;

use crate::error::Error;
use crate::error::Result;
use crate::server::tcp_session::TCPSession;
use crate::sql::engine::KVEngine;
use crate::storage::mvcc::MVCC;
use crate::storage::Store;

pub struct Server {
    id: String,
    engine: KVEngine,
    sql_listener: Option<TcpListener>,
}

impl Server {
    /// create a new db server
    pub async fn new(id: &str, sql_store: Box<dyn Store>) -> Result<Self> {
        Ok(Self {
            id: id.to_string(),
            engine: KVEngine {
                mvcc: MVCC::new(sql_store),
            },
            sql_listener: None,
        })
    }

    /// start listening on the given ports, must be call before serve
    pub async fn listen(mut self, sql_addr: &str) -> Result<Self> {
        let sql = TcpListener::bind(sql_addr).await?;
        info!(
            "SQL Server id {} Listening on {} (SQL)",
            self.id,
            sql.local_addr()?
        );
        self.sql_listener = Some(sql);
        Ok(self)
    }

    /// serve SQL request until the returned future is dropped. Consumes the server
    pub async fn server(self) -> Result<()> {
        let sql_listener = self
            .sql_listener
            .ok_or_else(|| Error::Internal("Must listen before serving".into()))?;
        tokio::try_join!(Self::sql_serve(sql_listener, self.engine),)?;
        Ok(())
    }

    /// server sql
    async fn sql_serve(listener: TcpListener, engine: KVEngine) -> Result<()> {
        let mut listener = TcpListenerStream::new(listener);
        // a client connectioned
        while let Some(socket) = listener.try_next().await? {
            let peer = socket.peer_addr()?;
            let session = TCPSession::new(engine.clone())?;
            tokio::spawn(async move {
                info!("Client {} connected", peer);
                match session.handle(socket).await {
                    Ok(()) => info!("Client {} disconnected", peer),
                    Err(e) => error!("Client {} error: {}", peer, e),
                }
            });
        }
        todo!()
    }
}
