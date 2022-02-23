use serde_derive::Deserialize;
use serde_derive::Serialize;
use sqlparser::ast::ColumnDef;
use sqlparser::ast::ColumnOption;

use super::data_type::DataType;
use super::data_value::DataValue;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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
}
