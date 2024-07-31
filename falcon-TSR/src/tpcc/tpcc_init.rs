use crate::config::POOL_PERC;
use crate::storage::catalog::Catalog;
use crate::storage::schema::{ColumnType, TableSchema};
use std::path::Path;

pub fn init_schema(file_name: impl AsRef<Path>) {
    let catalog = Catalog::global();
    let contents = std::fs::read_to_string(file_name).unwrap();
    let lines = contents.split("\n");
    let mut state = 0;
    let mut schema = TableSchema::new();
    let mut table_name = "";
    let mut _column_name = "";
    // state = 0: wait
    // state = 1: load table
    // state = 2: load index

    for line in lines {
        match state {
            0 => {
                if line.starts_with("TABLE") {
                    table_name = &line[6..];
                    schema = TableSchema::new();
                    state = 1;
                }
                if line.starts_with("INDEX") {
                    state = 2;
                    _column_name = &line[6..];
                }
                if line.starts_with("RINDEX") {
                    state = 3;
                    _column_name = &line[6..];
                }
            }
            1 => {
                if line.len() < 5 {
                    state = 0;
                    catalog.add_table3(table_name, schema.clone()).unwrap();
                    continue;
                }
                let s: Vec<&str> = line[1..].split(",").collect();
                match s[1] {
                    "int64_t" => schema.push(ColumnType::Int64, s[2]),
                    "string" => {
                        let len = s[0].to_string();
                        schema.push(
                            ColumnType::String {
                                len: len.parse::<usize>().unwrap(),
                            },
                            s[2],
                        )
                    }
                    "double" => schema.push(ColumnType::Double, s[2]),
                    _ => {
                        println!("invalid type {}", s[1]);
                    }
                }
            }
            2 => {
                if line.len() < 5 {
                    state = 0;
                    continue;
                }
                let s: Vec<&str> = line[0..].split(",").collect();
                table_name = s[0];
                println!("{}", table_name);
                catalog.set_primary_key3(table_name, 0);
            }
            3 => {
                if line.len() < 5 {
                    state = 0;
                    continue;
                }
                let s: Vec<&str> = line[0..].split(",").collect();
                table_name = s[0];
                println!("{}", table_name);
                catalog.set_range_primary_key3(table_name, 3);
            }
            _ => {
                assert!(false);
            }
        }
    }
    catalog.add_index_by_name3("CUSTOMER", "C_LAST");
    catalog.set_primary_key3("ORDER", 0);
    catalog.add_index_by_name3("ORDER", "O_C_ID");

    catalog.set_range_primary_key3("NEW-ORDER", 0);
    #[cfg(feature = "buffer_pool")]
    {
        use crate::tpcc::*;
        catalog.set_pool_size3("WAREHOUSE", 400000 as usize); // 2048
        catalog.set_pool_size3("DISTRICT", 4000000 as usize); // 20480
        catalog.set_pool_size3(
            "CUSTOMER",
            (WAREHOUSES * DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT) as usize,
        ); // 6144_0000
        catalog.set_pool_size3("ORDER", 2000000 as usize ); // 6144_0000
        catalog.set_pool_size3("NEW-ORDER", 1000000);
        catalog.set_pool_size3("ITEM", 10_000_000 as usize); // 100_000
        catalog.set_pool_size3(
            "STOCK",
            (WAREHOUSES * STOCKS_PER_WAREHOUSE) as usize ,
        ); // 2_0480_0000
        catalog.set_pool_size3("ORDER-LINE", 10000000); // 6144_0000
        catalog.set_pool_size3("HISTORY", 10000000); //
    }
}
