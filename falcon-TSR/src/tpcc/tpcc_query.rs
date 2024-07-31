use chrono::Local;

use crate::storage::catalog::Catalog;
use crate::storage::table::Table;
use crate::tpcc::*;
use crate::transaction::transaction::Transaction;
use crate::transaction::transaction_buffer::TransactionBuffer;
use std::sync::Arc;

#[derive(Debug)]
pub struct Item {
    pub iid: u64,
    pub wid: u64,
    pub quantity: u64,
}
impl Item {
    pub fn new(iid: u64, wid: u64, quantity: u64) -> Self {
        Item {
            iid: iid,
            wid: wid,
            quantity: quantity,
        }
    }
}
#[derive(Debug)]
pub struct TpccQuery {
    //basic
    pub wid: u64,
    pub did: u64,
    pub cid: u64,
    //New Order
    pub items: Vec<Item>,
    pub entry_d: u64,
    //Payment
    pub c_wid: u64,
    pub c_did: u64,
    pub c_last: String,
    pub by_last: bool,
    pub h_amount: f64,
    // STOCK-LEVEL
    pub threshold: u64,
    // DELEVER
    pub o_carrier_id: u64,
    pub ol_delivery_d: u64,
}
impl TpccQuery {
    fn wid_for_thread(rng: &mut ThreadRng, thread_id: u64) -> u64 {
        let k1 = (WAREHOUSES - thread_id - 1) / (TRANSACTION_COUNT as u64);
        let k2 = u64_rand(rng, 0, k1) * (TRANSACTION_COUNT as u64);
        k2 + thread_id
        // u64_rand(rng, 0, WAREHOUSES)
    }
    pub fn gen_new_order(rng: &mut ThreadRng, thread_id: u64) -> Self {
        let wid = TpccQuery::wid_for_thread(rng, thread_id);
        let did = u64_rand(rng, 1, DISTRICTS_PER_WAREHOUSE) - 1;
        let cid = nurand(rng, 1023, 1, CUSTOMERS_PER_DISTRICT) - 1;
        let ol_cnt = u64_rand(rng, 5, 15);
        let rbk = u64_rand(rng, 1, 100);
        let mut items: Vec<Item> = Vec::new();
        items.reserve(ol_cnt as usize);
        for olid in 0..ol_cnt {
            let mut ol_iid = ITEMS - 1;
            if olid + 1 < ol_cnt || rbk > 1 {
                loop {
                    ol_iid = nurand(rng, 8191, 1, ITEMS) - 1;
                    let mut flag = true;
                    for i in 0..olid {
                        if items[i as usize].iid == ol_iid {
                            flag = false;
                            break;
                        }
                    }
                    if flag {
                        break;
                    }
                }
            }
            let mut ol_wid = wid;
            if u64_rand(rng, 1, 100) == 1 && WAREHOUSES > 1 {
                loop {
                    ol_wid = u64_rand(rng, 1, WAREHOUSES) - 1;
                    if ol_wid != wid {
                        break;
                    }
                }
            }
            let item = Item::new(ol_iid, ol_wid, u64_rand(rng, 1, 10));
            items.push(item);
        }
        TpccQuery {
            wid: wid,
            did: did,
            cid: cid,
            items: items,
            entry_d: 0,
            c_wid: wid,
            c_did: did,
            c_last: String::new(),
            by_last: false,
            h_amount: 0.00,
            threshold: 0,
            o_carrier_id: 0,
            ol_delivery_d: 0,
        }
    }
    pub fn gen_payment(rng: &mut ThreadRng, thread_id: u64) -> Self {
        let wid = TpccQuery::wid_for_thread(rng, thread_id);
        let did = u64_rand(rng, 1, DISTRICTS_PER_WAREHOUSE) - 1;
        let mut c_wid = wid;
        let mut c_did = did;
        if u64_rand(rng, 1, 100) > 85 {
            if WAREHOUSES > 1 {
                loop {
                    c_wid = u64_rand(rng, 1, WAREHOUSES) - 1;
                    if c_wid != wid {
                        break;
                    }
                }
            }
            c_did = u64_rand(rng, 1, DISTRICTS_PER_WAREHOUSE) - 1;
        }
        let c_last: String;
        let cid: u64;
        let by_last = u64_rand(rng, 1, 100) <= 60;
        if by_last {
            c_last = lastname(nurand(rng, 255, 0, 999));
            cid = CUSTOMERS_PER_DISTRICT;
        } else {
            c_last = String::new();
            cid = nurand(rng, 1023, 1, CUSTOMERS_PER_DISTRICT) - 1;
        }
        let h_amount = f64_rand(rng, 1.00, 5000.00, 0.01);
        TpccQuery {
            wid: wid,
            did: did,
            cid: cid,
            items: Vec::new(),
            entry_d: 0,
            c_wid: c_wid,
            c_did: c_did,
            c_last: c_last,
            by_last: by_last,
            h_amount: h_amount,
            threshold: 0,
            o_carrier_id: 0,
            ol_delivery_d: 0,
        }
    }
    pub fn gen_order_status(rng: &mut ThreadRng, thread_id: u64) -> Self {
        let wid = TpccQuery::wid_for_thread(rng, thread_id);
        let did = u64_rand(rng, 1, DISTRICTS_PER_WAREHOUSE) - 1;
        let c_last: String;
        let cid: u64;
        let by_last = u64_rand(rng, 1, 100) <= 60;
        if by_last {
            c_last = lastname(nurand(rng, 255, 0, 999));
            cid = CUSTOMERS_PER_DISTRICT;
        } else {
            c_last = String::new();
            cid = nurand(rng, 1023, 1, CUSTOMERS_PER_DISTRICT) - 1;
        }
        TpccQuery {
            wid: wid,
            did: did,
            cid: cid,
            items: Vec::new(),
            entry_d: 0,
            c_wid: 0,
            c_did: 0,
            c_last: c_last,
            by_last: by_last,
            h_amount: 0.00,
            threshold: 0,
            o_carrier_id: 0,
            ol_delivery_d: 0,
        }
    }
    pub fn gen_stock_level(rng: &mut ThreadRng, thread_id: u64) -> Self {
        let wid = TpccQuery::wid_for_thread(rng, thread_id);
        let did = u64_rand(rng, 1, DISTRICTS_PER_WAREHOUSE) - 1;
        let threshold = u64_rand(rng, 10, 20);
        TpccQuery {
            wid: wid,
            did: did,
            cid: 0,
            items: Vec::new(),
            entry_d: 0,
            c_wid: 0,
            c_did: 0,
            c_last: String::new(),
            by_last: false,
            h_amount: 0.00,
            threshold: threshold,
            o_carrier_id: 0,
            ol_delivery_d: 0,
        }
    }

    pub fn gen_deliver(rng: &mut ThreadRng, thread_id: u64) -> Self {
        let wid = TpccQuery::wid_for_thread(rng, thread_id);
        let o_carrier_id = u64_rand(rng, 1, DISTRICTS_PER_WAREHOUSE);
        let ol_delivery_d = Local::now().timestamp_nanos() as u64;
        TpccQuery {
            wid: wid,
            did: 0,
            cid: 0,
            items: Vec::new(),
            entry_d: 0,
            c_wid: 0,
            c_did: 0,
            c_last: String::new(),
            by_last: false,
            h_amount: 0.00,
            threshold: 0,
            o_carrier_id: o_carrier_id,
            ol_delivery_d: ol_delivery_d,
        }
    }
}

pub struct TableList {
    pub warehouses: [Arc<Table>; 3],
    pub districts: [Arc<Table>; 3],
    pub customers: [Arc<Table>; 3],
    pub orders: [Arc<Table>; 3],
    pub new_orders: [Arc<Table>; 3],
    pub items: [Arc<Table>; 3],
    pub stocks: [Arc<Table>; 3],
    pub order_lines: [Arc<Table>; 3],
    pub histories: [Arc<Table>; 3],
}

impl TableList {
    pub fn new(catalog: &Catalog) -> TableList {
        TableList {
            warehouses: catalog.get_table3("WAREHOUSE"),
            districts: catalog.get_table3("DISTRICT"),
            customers: catalog.get_table3("CUSTOMER"),
            orders: catalog.get_table3("ORDER"),
            new_orders: catalog.get_table3("NEW-ORDER"),
            items: catalog.get_table3("ITEM"),
            stocks: catalog.get_table3("STOCK"),
            order_lines: catalog.get_table3("ORDER-LINE"),
            histories: catalog.get_table3("HISTORY"),
        }
    }
    #[cfg(feature = "buffer_pool")]
    pub fn apply_tuple(&self, index: usize, thread_id: usize, reset: bool, txn: &mut Transaction) {
        self.warehouses[index].apply_tuple(thread_id, reset, txn);
        self.districts[index].apply_tuple(thread_id, reset, txn);
        self.customers[index].apply_tuple(thread_id, reset, txn);
        self.orders[index].apply_tuple(thread_id, reset, txn);
        self.new_orders[index].apply_tuple(thread_id, reset, txn);
        self.items[index].apply_tuple(thread_id, reset, txn);
        self.stocks[index].apply_tuple(thread_id, reset, txn);
        self.order_lines[index].apply_tuple(thread_id, reset, txn);
        self.histories[index].apply_tuple(thread_id, reset, txn);
    }
    #[cfg(feature = "buffer_pool")]
    pub fn apply_replica_tuple(
        &self,
        src_index: usize,
        dst_index: usize,
        thread_id: usize,
        reset: bool,
        txn: &mut Transaction,
    ) {
        self.warehouses[src_index].apply_replica_tuple(
            thread_id,
            &self.warehouses[dst_index],
            reset,
            txn,
        );
        self.districts[src_index].apply_replica_tuple(
            thread_id,
            &self.districts[dst_index],
            reset,
            txn,
        );
        self.customers[src_index].apply_replica_tuple(
            thread_id,
            &self.customers[dst_index],
            reset,
            txn,
        );
        self.orders[src_index].apply_replica_tuple(thread_id, &self.orders[dst_index], reset, txn);
        self.new_orders[src_index].apply_replica_tuple(
            thread_id,
            &self.new_orders[dst_index],
            reset,
            txn,
        );
        self.items[src_index].apply_replica_tuple(thread_id, &self.items[dst_index], reset, txn);
        self.stocks[src_index].apply_replica_tuple(thread_id, &self.stocks[dst_index], reset, txn);
        self.order_lines[src_index].apply_replica_tuple(
            thread_id,
            &self.order_lines[dst_index],
            reset,
            txn,
        );
        self.histories[src_index].apply_replica_tuple(
            thread_id,
            &self.histories[dst_index],
            reset,
            txn,
        );
    }
    #[cfg(feature = "buffer_pool")]
    pub fn clear(&self, index: usize, thread_id: usize) {
        self.warehouses[index].clear(thread_id);
        self.districts[index].clear(thread_id);
        self.customers[index].clear(thread_id);
        self.orders[index].clear(thread_id);
        self.new_orders[index].clear(thread_id);
        self.items[index].clear(thread_id);
        self.stocks[index].clear(thread_id);
        self.order_lines[index].clear(thread_id);
        self.histories[index].clear(thread_id);
    }
}
