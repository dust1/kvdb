use serde_derive::Deserialize;
use serde_derive::Serialize;
use sqlparser::ast::ColumnDef;
use sqlparser::ast::ColumnOption;

use super::data_type::DataType;
use super::data_value::DataValue;
use super::table::Table;
use crate::error::Error;
use crate::error::Result;
use crate::sql::engine::SQLTransaction;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableColumn {
    /// Column name
    pub name: String,
    /// Column datatype
    pub datatype: DataType,
    /// Whether the column is a primary key
    pub primary_key: bool,
    /// Whether the column allows null values
    pub nullable: bool,
    /// The default value of the column
    pub default: Option<DataValue>,
    /// Whether the column should only take unique values
    pub unique: bool,
    /// The table which is referenced by this foreign key, link other table's primary key
    pub references: Option<String>,
    /// Whether the column should be indexed
    pub index: bool,
}

impl TableColumn {
    pub fn try_form(column_def: &ColumnDef) -> Self {
        let mut column = Self {
            name: column_def.name.to_string(),
            datatype: DataType::try_form(&column_def.data_type),
            primary_key: false,
            nullable: false,
            default: None,
            unique: false,
            references: None,
            index: false,
        };

        for column_d in &column_def.options {
            match &column_d.option {
                ColumnOption::Null => column.nullable = true,
                ColumnOption::NotNull => column.nullable = false,
                ColumnOption::Default(expr) => column.default = Some(DataValue::from_expr(expr)),
                ColumnOption::Unique { is_primary: true } => {
                    column.unique = true;
                    column.primary_key = true;
                    column.index = true;
                }
                ColumnOption::Unique { .. } => column.unique = true,
                ColumnOption::ForeignKey { foreign_table, .. } => {
                    column.references = Some(foreign_table.to_string())
                }
                _ => {}
            }
        }
        column
    }

    /// validate column value
    pub fn validate_value(
        &self,
        table: &Table,
        pk: &DataValue,
        value: &DataValue,
        txn: &mut dyn SQLTransaction,
    ) -> Result<()> {
        // validate type
        match value.data_type() {
            None if self.nullable => Ok(()),
            None => Err(Error::Value(format!(
                "NULL value not allowed for column {}",
                self.name
            ))),
            Some(ref datatype) if datatype != &self.datatype => Err(Error::Value(format!(
                "Invalid datatype {} for {} column",
                datatype, self.name
            ))),
            _ => Ok(()),
        }?;

        // validate value
        match value {
            DataValue::String(s) if s.len() > 1024 => {
                Err(Error::Value("String cannot be more than 1024 bytes".into()))
            }
            _ => Ok(()),
        }?;

        if let Some(target) = &self.references {
            match value {
                DataValue::Null => Ok(()),
                DataValue::Float(f) if f.is_nan() => Ok(()),
                v if target == &table.name && v == pk => Ok(()),
                v if txn.read(target, v)?.is_none() => Err(Error::Value(format!(
                    "Reference primary key {} in table {} does not exist",
                    v, target
                ))),
                _ => Ok(()),
            }?;
        }

        // validate uniquenes constrains
        if self.unique && !self.primary_key && value != &DataValue::Null {
            let index = table.get_column_index(&self.name)?;
            let mut scan = txn.scan(&table.name, None)?;
            while let Some(row) = scan.next().transpose()? {
                if value == &row[index] && pk != &table.get_row_key(&row)? {
                    return Err(Error::Value(format!(
                        "Unique column {}, the value {} has exist",
                        self.name, value
                    )));
                }
            }
        }

        Ok(())
    }

    /// validate column schema
    pub fn validate(&self, table: &Table, txn: &mut dyn SQLTransaction) -> Result<()> {
        // validate primary key, the key should not be null and unique
        if self.primary_key && self.nullable {
            return Err(Error::Value(format!(
                "Table {}, column {} is primary key, it can't be nullable!!",
                table.name, self.name
            )));
        }
        if self.primary_key && !self.unique {
            return Err(Error::Value(format!(
                "Table {}, column {} is primary key, it should be unique!!",
                table.name, self.name
            )));
        }

        // validate default value
        if let Some(default) = &self.default {
            if let Some(datatype) = default.data_type() {
                if datatype != self.datatype {
                    return Err(Error::Value(format!(
                        "Table {}, column {} default datatype {} must be {}",
                        table.name, self.name, datatype, self.datatype
                    )));
                }
            } else if !self.nullable {
                return Err(Error::Value(format!(
                    "Can not use NULL as default value for non-nullable column {}",
                    self.name
                )));
            }
        } else if self.nullable {
            return Err(Error::Value(format!(
                "Nullable column {} must have a default value",
                self.name
            )));
        }

        // validate reference
        if let Some(reference) = &self.references {
            let target = if reference == &table.name {
                // reference self
                table.clone()
            } else if let Some(table) = txn.read_table(reference)? {
                table
            } else {
                return Err(Error::Value(format!(
                    "Table {} reference by column {} does not exist",
                    reference, self.name
                )));
            };

            if self.datatype != target.get_primary_key()?.datatype {
                return Err(Error::Value(format!(
                    "Can't reference {} primary key of table {} from {} column {}",
                    target.get_primary_key()?.datatype,
                    target.name,
                    self.datatype,
                    self.name
                )));
            }
        }
        Ok(())
    }
}
