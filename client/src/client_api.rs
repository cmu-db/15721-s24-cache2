use anyhow::Result;
use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc::Receiver;

/// Id types for table, column, and record. Need to be consistent among all components
/// (e.g. execution engine). We don't want to make any type generic here just for the id,
/// so we simply define them here. Might refine later.
pub type TableId = u64;
pub type ColumnId = u64;
pub type RecordId = u64;

/// Id type for the request. Should be unique among all requests.
pub type RequestId = usize;

/// [`StorageRequest`] is the request that the execution engine sends to the storage node.
#[derive(Clone)]
pub struct StorageRequest {
    request_id: RequestId,
    data_request: DataRequest,
}

impl StorageRequest {
    pub fn new(request_id: RequestId, data_request: DataRequest) -> Self {
        Self {
            request_id,
            data_request,
        }
    }

    pub fn request_id(&self) -> RequestId {
        self.request_id
    }

    pub fn data_request(&self) -> &DataRequest {
        &self.data_request
    }
}

/// [`DataRequest`] specifies the requests that the execution engine might issue to
/// the storage node.
///
/// Currently we assume the execution engine only requests the whole table/column. We may
/// add `std::ops::RangeBounds` later to support range query from the execution engine.
#[derive(Clone)]
pub enum DataRequest {
    /// Requests a whole table from the underlying storage.
    Table(TableId),
    /// Requests one or more columns from the underlying storage.
    Columns(TableId, Vec<ColumnId>),
    /// Requests one or more tuples from the underlying storage.
    /// FIXME: Do we really need this?
    Tuple(Vec<RecordId>),
}

/// [`StorageClient`] provides the interface for the execution engine to query data from the
/// storage node. It resolves the physical location of the tables/columns/tuples by querying
/// the catalog node, and then sends the request to the storage node to get the data from the
/// underlying storage.
#[async_trait::async_trait]
pub trait StorageClient: Send + Sync + 'static {
    /// Returns the requested data as a stream.
    async fn request_data(&mut self, request: StorageRequest) -> Result<Receiver<RecordBatch>>;

    /// Returns all the requested data as a whole.
    async fn request_data_sync(&mut self, request: StorageRequest) -> Result<Vec<RecordBatch>>;
}
