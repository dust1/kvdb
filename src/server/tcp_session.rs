use futures::sink::SinkExt as _;

use log::info;
use tokio::net::TcpStream;
use tokio_stream::StreamExt as _;
use tokio_util::codec::Framed;
use tokio_util::codec::LengthDelimitedCodec;

use super::servlet::Request;
use super::servlet::Response;
use crate::common::result::ResultSet;
use crate::error::Result;
use crate::sql::engine::Catalog;
use crate::sql::engine::KVEngine;
use crate::sql::engine::SQLEngine;
use crate::sql::engine::SQLSession;
use crate::storage::mvcc::TransactionMode;

/// a client session coupled to a SQL session
pub struct TCPSession {
    engine: KVEngine,
    session: SQLSession<KVEngine>,
}

impl TCPSession {
    pub fn new(engine: KVEngine) -> Result<Self> {
        Ok(Self {
            session: engine.session()?,
            engine,
        })
    }

    pub async fn handle(mut self, socket: TcpStream) -> Result<()> {
        let mut stream = tokio_serde::Framed::new(
            Framed::new(socket, LengthDelimitedCodec::new()),
            tokio_serde::formats::Bincode::default(),
        );

        while let Some(request) = stream.try_next().await? {
            info!("request info {:?}", request);
            let mut response = self.request(request);

            let mut rows: Box<dyn Iterator<Item = Result<Response>> + Send> =
                Box::new(std::iter::empty());

            if let Ok(Response::Execute(ResultSet::Query {
                rows: ref mut resultrows,
                ..
            })) = &mut response
            {
                rows = Box::new(
                    std::mem::replace(resultrows, Box::new(std::iter::empty()))
                        .map(|result| result.map(|row| Response::Row(Some(row))))
                        .chain(std::iter::once(Ok(Response::Row(None))))
                        .scan(false, |err_sent, response| match (&err_sent, &response) {
                            (true, _) => None,
                            (_, Err(error)) => {
                                *err_sent = true;
                                Some(Err(error.clone()))
                            }
                            _ => Some(response),
                        })
                        .fuse(),
                );
            }

            stream.send(response).await?;
            stream
                .send_all(&mut tokio_stream::iter(rows.map(Ok)))
                .await?;
        }

        Ok(())
    }

    /// execute a request
    pub fn request(&mut self, request: Request) -> Result<Response> {
        Ok(match request {
            Request::Execute(query) => Response::Execute(self.session.execute(&query)?),
            Request::ListTables => {
                Response::ListTable(self.session.with_txn(TransactionMode::ReadOnly, |txn| {
                    Ok(txn.scan_table()?.map(|t| t.name).collect())
                })?)
            }
            _ => Response::ListTable(vec!["unsupport".into()]),
        })
    }
}

impl Drop for TCPSession {
    fn drop(&mut self) {
        self.session.execute("ROLLBACK").ok();
    }
}
