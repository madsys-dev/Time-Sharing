use bytes::Buf;
use dashmap::DashMap;

use crate::storage::{row::BufferDataVec, schema::TableSchema, table::TupleId};

#[derive(Debug)]
pub struct LocalTable {
    tuples: DashMap<TupleId, BufferDataVec>,
    tuple_size: usize,
    schema: TableSchema,
}

impl LocalTable {
    pub fn new(tuple_size: usize, schema: TableSchema) -> Self {
        LocalTable {
            tuples: DashMap::new(),
            tuple_size,
            schema,
        }
    }
    pub fn clear(&mut self) {
        self.tuples.clear();
    }
    pub fn find(
        &self,
        tuple_id: &TupleId,
    ) -> Option<dashmap::mapref::one::Ref<TupleId, BufferDataVec>> {
        let result = self.tuples.get(tuple_id);
        match result {
            Some(tuple) => {
                return Some(tuple);
            }
            None => {
                return None;
            }
        }
    }
    pub fn insert(&self, tuple_id: &TupleId) -> dashmap::mapref::one::Ref<TupleId, BufferDataVec> {
        let buffer = BufferDataVec::new(self.tuple_size);
        self.tuples.insert(tuple_id.clone(), buffer);
        return self.tuples.get(tuple_id).unwrap();
    }
}
