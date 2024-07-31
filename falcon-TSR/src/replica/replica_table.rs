use std::sync::Arc;

use crate::storage::table::Table;

use super::local_table::LocalTable;

#[derive(Debug)]
pub struct ReplicaTable {
    pub local_table: LocalTable,
    pub tables: Vec<Arc<Table>>,
    pub primary_id: usize,
}

impl ReplicaTable {
    pub fn new(tables: Vec<Arc<Table>>) -> ReplicaTable {
        let table = tables.get(0).unwrap();
        ReplicaTable {
            local_table: LocalTable::new(table.tuple_size as usize, table.schema.clone()),
            tables,
            primary_id: 0,
        }
    }
    pub fn write(&self, rid: usize) {}
}
