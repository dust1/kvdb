use std::cell::Cell;
use std::sync::Arc;

use tokio::net::TcpStream;
use tokio::net::ToSocketAddrs;
use tokio::sync::Mutex;
use tokio_util::codec::Framed;
use tokio_util::codec::LengthDelimitedCodec;

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
    pub async fn new<A: ToSocketAddrs>(_addr: A) -> Result<Self> {
        todo!()
    }

    /// return the transaction status of the client
    pub fn txn(&self) -> Option<(u64, TransactionMode)> {
        self.txn.get()
    }
}
