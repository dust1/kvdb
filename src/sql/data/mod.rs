mod data_column;
mod data_type;
mod data_value;

pub use data_column::DataColumn;
pub use data_type::DataType;
pub use data_value::DataValue;

use crate::error::Result;

pub type DataRow = Vec<DataValue>;
pub type DataRows = Box<dyn Iterator<Item = Result<DataRow>> + Send>;
pub type DataColumns = Vec<DataColumn>;
