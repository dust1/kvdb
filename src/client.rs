use std::cell::Cell;
use std::sync::Arc;

use futures::SinkExt;
use tokio::net::TcpStream;
use tokio::net::ToSocketAddrs;
use tokio::sync::Mutex;
use tokio::sync::MutexGuard;
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;
use tokio_util::codec::LengthDelimitedCodec;

use crate::common::result::ResultSet;
use crate::error::Error;
use crate::error::Result;
use crate::server::servlet::Request;
use crate::server::servlet::Response;
use crate::storage::mvcc::TransactionMode;

type Connection = tokio_serde::Framed<
    Framed<TcpStream, LengthDelimitedCodec>,
    Result<Response>,
    Request,
    tokio_serde::formats::Bincode<Result<Response>, Request>,
>;

/// client
#[derive(Clone)]
pub struct Client {
    conn: Arc<Mutex<Connection>>,
    txn: Cell<Option<(u64, TransactionMode)>>,
}

impl Client {
    pub async fn new<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        Ok(Self {
            conn: Arc::new(Mutex::new(tokio_serde::Framed::new(
                Framed::new(TcpStream::connect(addr).await?, LengthDelimitedCodec::new()),
                tokio_serde::formats::Bincode::default(),
            ))),
            txn: Cell::new(None),
        })
    }

    /// return the transaction status of the client
    pub fn txn(&self) -> Option<(u64, TransactionMode)> {
        self.txn.get()
    }

    pub async fn execute(&self, query: &str) -> Result<ResultSet> {
        let mut conn = self.conn.lock().await;
        let mut result_set = match self
            .call_locked(&mut conn, Request::Execute(query.into()))
            .await?
        {
            Response::Execute(rs) => rs,
            resp => return Err(Error::Internal(format!("Unecpected response: {:?}", resp))),
        };

        if let ResultSet::Query { columns, .. } = result_set {
            // the ResultSet::Query, column and rows are separated
            let mut rows = Vec::new();
            while let Some(result) = conn.try_next().await? {
                match result? {
                    Response::Row(Some(row)) => rows.push(row),
                    Response::Row(None) => break,
                    response => {
                        return Err(Error::Internal(format!(
                            "Unexcepted response: {:?}",
                            response
                        )))
                    }
                }
            }
            result_set = ResultSet::Query {
                columns,
                rows: Box::new(rows.into_iter().map(Ok)),
            }
        }

        Ok(result_set)
    }

    /// call a server method while holding the mutex lock
    async fn call_locked(
        &self,
        conn: &mut MutexGuard<'_, Connection>,
        request: Request,
    ) -> Result<Response> {
        conn.send(request).await?;
        match conn.try_next().await? {
            Some(result) => result,
            None => Err(Error::Internal("Server disconnected".into())),
        }
    }
}
