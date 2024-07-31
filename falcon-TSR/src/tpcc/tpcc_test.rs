use crate::config::BATCH_SIZE;
use crate::storage::catalog::Catalog;
use crate::storage::global::{Timer, READING};
use crate::storage::nvm_file::NVMTableStorage;
use crate::tpcc::tpcc::*;
use crate::{tpcc::*, utils};
// use crate::tpcc::tpcc_index::TpccIndex;
use crate::mvcc_config::THREAD_COUNT;
use crate::config::{TEST_THREAD_COUNT, TRANSACTION_COUNT};
use crate::tpcc::tpcc_init;
use crate::tpcc::tpcc_query::*;
use crate::tpcc::tpcc_txn_sycn;
use crate::transaction::transaction::Transaction;
use crate::transaction::transaction_buffer::TransactionBuffer;
use crate::utils::executor::executor::Executor;
use log4rs;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::sync::{mpsc, Barrier};
use std::time::{Duration, SystemTime};
use std::{thread, time};

pub fn tpcc_test() {
    let root_addr = NVMTableStorage::init_test_database();
    println!("init database");

    Catalog::init_catalog(root_addr);
    if IS_FULL_SCHEMA {
        tpcc_init::init_schema("config/schema_file/TPCC_full_schema.txt");
    } else {
        tpcc_init::init_schema("config/schema_file/TPCC_short_schema.txt");
    }
    init_tables();
    // let threads::Vec<JoinHandle> = Vec::new();
    let mut f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open("latency.txt")
        .unwrap();
    let (tx0, rx) = mpsc::channel();
    // let senders = Vec::new();
    println!("test start");

    let catalog = Catalog::global();
    let total_time = Duration::new(10, 0);

    // for i in 0..TRANSACTION_COUNT {
    //     let mut buffer = TransactionBuffer::new(catalog, i as u64);
    //     let mut txn = Transaction::new(&mut buffer, false);
    //     txn.begin();
    //     txn.commit();
    // }
    let barrier: Arc<Barrier> = Arc::new(Barrier::new(TEST_THREAD_COUNT+1));
    let cb: Arc<Barrier> = barrier.clone();

    let f_counter = Arc::new(AtomicU64::new(0));
    let finish = f_counter.clone();
    thread::spawn(move || {
        cb.wait();
        let start: SystemTime = SystemTime::now();
        std::thread::sleep(Duration::from_secs(30));
        loop {
            let end = SystemTime::now();
            let during: Duration = end.duration_since(start).unwrap();
            if during.ge(&total_time) {
                println!("{:?}", during);
                break;
            }
            utils::wait_for_nanos(100000);
        }
        finish.fetch_add(1, Ordering::Relaxed);
        // for thread_id in 0..TEST_THREAD_COUNT {
        //     let address = catalog.get_transaction_page_start(thread_id as u64);
        //     let buffer = TransactionBuffer {
        //         thread_id: 0,
        //         offset: 0,
        //         address,
        //     };
        //     buffer.stop();
        //     println!("{} {} stop", thread_id, address);
        // }
    });
    let table_versions = Arc::new([AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)]);
    for i in 0..TEST_THREAD_COUNT {
        let tx = tx0.clone();
        let b = barrier.clone();
        let table_version = table_versions.clone();
        let finished: Arc<AtomicU64> = f_counter.clone();
        thread::spawn(move || {
            let mut buffer = TransactionBuffer::new(catalog, i as u64);
            println!("test prepare {} {}", i, buffer.address);
            let mut rng = rand::thread_rng();
            let mut txn = Transaction::new(&mut buffer, false);

            #[cfg(feature = "txn_clock")]
            let mut timer = Timer::new();

            let tablelist = TableList::new(&catalog);

            #[cfg(feature = "nbtree")]
            crate::storage::index::nbtree::init_index((i + THREAD_COUNT) as i32);
            let mut num = 0;
            let mut total = 0;
            println!("test start {} ", i);
            b.wait();
            let start = SystemTime::now();
            let batch_size = unsafe { BATCH_SIZE };
            let mut state = (i & 1) * 4;
            let mut version: u64 = ((i & 1) as u64) * TEST_THREAD_COUNT as u64 / 2;
            
            let mut wait_time = 0;
            #[cfg(not(feature = "time_sharding"))]
            {
                state = 0;
                version = 0;
            }
            loop {
                match state {
                    0 => {
                        if version >= TEST_THREAD_COUNT as u64 {
                            table_version[2].fetch_add(1, Ordering::Relaxed);
                            utils::file::sfence();
                            // timer.end(2, 2);
                        }
                        // b.wait();
                        // println!("thread {}, state {}", i, state);

                        // let a1 = SystemTime::now();
                        // timer.start(0);
                        // #[cfg(feature = "time_sharding")]
                        while table_version[0].load(Ordering::Relaxed) < version {
                            #[cfg(not(feature = "time_sharding"))]
                            if unsafe { BATCH_SIZE } == 1 {
                                break;
                            }
                            // println!("thread {}, state {}, version {}, t1.ver: {}", i, state, version, table[1].get_version());
                            utils::wait_for_nanos(500);

                            if finished.load(Ordering::Relaxed) > 0 {
                                break;
                            }
                        }

                        // let a2 = SystemTime::now();
                        // if version == 256 {
                        //     println!("tsp wait p time {:?}", a2.duration_since(a1).unwrap());
                        // }
                        // timer.add_tmp(0, 0);

                        // timer.start(1);
                        let a1: SystemTime = SystemTime::now();

                        loop {

                            let pr = u64_rand(&mut rng, 1, 100);
                            if pr <= 43 {
                                let query = TpccQuery::gen_payment(&mut rng, i as u64);
        
                                loop {
                                    total = total + 1;
        
                                    #[cfg(feature = "txn_clock")]
                                    timer.start(READING);
                                    // println!("{} payment", i);
                                    if tpcc_txn_sycn::run_payment(&mut txn, &tablelist, &query) {
                                        num = num + 1;
        
                                        #[cfg(feature = "payment_clock")]
                                        timer.end(READING, READING);
                                        break;
                                    }
                                    let a2 = SystemTime::now();
                                    if a2.duration_since(a1).unwrap().as_nanos() > batch_size as u128 * 1000000 {
                                        break;
                                    }
                                }
                            } else if pr <= 88 {
                                let query = TpccQuery::gen_new_order(&mut rng, i as u64);
                                loop {
                                    total = total + 1;
        
                                    #[cfg(feature = "txn_clock")]
                                    timer.start(READING);
                                    // println!("{} new_order", i);
        
                                    if tpcc_txn_sycn::run_new_order(&mut txn, &tablelist, &query) {
                                        num = num + 1;
        
                                        #[cfg(feature = "new_order_clock")]
                                        timer.end(READING, READING);
                                        break;
                                    }
                                    let a2 = SystemTime::now();
                                    if a2.duration_since(a1).unwrap().as_nanos() > batch_size as u128 * 1000000 {
                                        break;
                                    }
                                }
                            } else if pr <= 92 {
                                let query = TpccQuery::gen_stock_level(&mut rng, i as u64);
                                loop {
                                    total = total + 1;
        
                                    #[cfg(feature = "txn_clock")]
                                    timer.start(READING);
                                    // println!("{} stock_level", i);
        
                                    if tpcc_txn_sycn::run_stock_level(&mut txn, &tablelist, &query) {
                                        num = num + 1;
        
                                        #[cfg(feature = "stock_level_clock")]
                                        timer.end(READING, READING);
                                        break;
                                    }
                                    let a2: SystemTime = SystemTime::now();
                                    if a2.duration_since(a1).unwrap().as_nanos() > batch_size as u128 * 1000000 {
                                        break;
                                    }
                                }
                            } else if pr <= 96 {
                                let query = TpccQuery::gen_order_status(&mut rng, i as u64);
                                loop {
                                    // println!("{} order_status", i);
                                    total = total + 1;
        
                                    #[cfg(feature = "txn_clock")]
                                    timer.start(READING);
                                    if tpcc_txn_sycn::run_order_status(&mut txn, &tablelist, &query) {
                                        num = num + 1;
                                        #[cfg(feature = "order_status_clock")]
                                        timer.end(READING, READING);
                                        break;
                                        
                                    }
                                    let a2: SystemTime = SystemTime::now();
                                    if a2.duration_since(a1).unwrap().as_nanos() > batch_size as u128 * 1000000 {
                                        break;
                                    }
                                }   
                            } else {
                                let query = TpccQuery::gen_deliver(&mut rng, i as u64);
                                loop {
                                    total = total + 1;
                                    // println!("{} deliver", i);
        
                                    #[cfg(feature = "txn_clock")]
                                    timer.start(READING);
                                    if tpcc_txn_sycn::run_deliver(&mut txn, &tablelist, &query) {
                                        num = num + 1;
                                        #[cfg(feature = "deliver_clock")]
                                        timer.end(READING, READING);
                                        break;
                                    }
                                    let a2: SystemTime = SystemTime::now();
                                    if a2.duration_since(a1).unwrap().as_nanos() > batch_size as u128 * 1000000 {
                                        break;
                                    }
                                }
                            }
                            let a2 = SystemTime::now();
                            if a2.duration_since(a1).unwrap().as_nanos() > batch_size as u128 * 1000000 {
                                break;
                            }
                        }
                        
                        // let a2 = SystemTime::now();
                        // if version == 256 {
                        //     println!("tsp execute time {:?}", a2.duration_since(a1).unwrap());
                        // }
                        // timer.end(1, 1);
                        state = 1;
                    }
                    1 => {
                        // b.wait();
                        // println!("thread {}, state {}", i, state);
                        #[cfg(feature = "buffer_pool")]
                        {
                            // timer.start(2);
                            tablelist.apply_tuple(0, i, false, &mut txn);
                            utils::file::sfence();
                            // let a2 = SystemTime::now();
                            // if version == 256 {
                            //     println!("tsp apply time {:?}", a2.duration_since(a1).unwrap());
                            // }
                        }
                        state = 2;
                    }
                    2 => {
                        // b.wait();
                        // println!("thread {}, state {}, version {}", i, state, version);
                        // let a1 = SystemTime::now();
                        table_version[0].fetch_add(1, Ordering::Relaxed);
                        utils::file::sfence();
                        // timer.end(2, 2);

                        // timer.start(2);
                        // TODO wait
                        #[cfg(feature = "buffer_pool")]
                        {
                            // timer.start(0);

                            while table_version[1].load(Ordering::Relaxed) < version {
                                #[cfg(not(feature = "time_sharding"))]
                                if unsafe { BATCH_SIZE } == 1 {
                                    break;
                                }
                                utils::wait_for_nanos(500);
                                if finished.load(Ordering::Relaxed) > 0 {
                                    break;
                                }
                            }
                            // let a2 = SystemTime::now();
                            // if version == 21 {
                            //     println!(
                            //         "tsp wait b time {:?}",
                            //         a2.duration_since(a1).unwrap()
                            //     );
                            // }
                            // timer.add_tmp(0, 0);
                            // timer.start(2);

                            tablelist.apply_replica_tuple(0, 1, i, false, &mut txn);

                            utils::file::sfence();
                        }
                        state = 3;
                    }
                    3 => {
                        // b.wait();
                        table_version[1].fetch_add(1, Ordering::Relaxed);
                        utils::file::sfence();
                        // timer.end(2, 2);
                        // println!("thread {}, state {}", i, state);
                        // TODO wait
                        #[cfg(feature = "buffer_pool")]
                        {
                            // timer.start(0);
                            // #[cfg(feature = "time_sharding")]
                            while table_version[2].load(Ordering::Relaxed) < version {
                                #[cfg(not(feature = "time_sharding"))]
                                if unsafe { BATCH_SIZE } == 1 {
                                    break;
                                }
                                utils::wait_for_nanos(500);
                                if finished.load(Ordering::Relaxed) > 0 {
                                    break;
                                }
                            }
                            // timer.total(0, 0, 0);

                            // timer.start(2);
                            tablelist.apply_replica_tuple(0, 2, i, true, &mut txn);
                            utils::file::sfence();
                        }
                        // #[cfg(not(feature = "time_sharding"))]
                        // {
                        //     version += 1;
                        // }
                        // #[cfg(feature = "time_sharding")]
                        // {
                        version += TEST_THREAD_COUNT as u64;
                        // }
                        // b.wait();
                        state = 0;
                    }
                    4 => {
                        // b.wait();
                        // println!("thread {}, state {}", i, state);
                        state = 5;
                    }
                    5 => {
                        // b.wait();
                        // println!("thread {}, state {}", i, state);
                        state = 0;
                    }
                    _ => {
                        assert!(false);
                    }
                }

                if finished.load(Ordering::Relaxed) > 0
                {
                    println!("222222 {}", i);
                    // finished.fetch_add(1, Ordering::Relaxed);
                    table_version[0].fetch_add(100000, Ordering::Relaxed);
                    table_version[1].fetch_add(100000, Ordering::Relaxed);
                    table_version[2].fetch_add(100000, Ordering::Relaxed);
                    #[cfg(feature = "buffer_pool")]
                    {
                        tablelist.apply_tuple(0, i, false, &mut txn);
                        // println!("22222 {}", i);

                        tablelist.apply_replica_tuple(0, 1, i, false, &mut txn);
                        tablelist.apply_replica_tuple(0, 2, i, true, &mut txn);
                        // tablelist.clear(0, i);
                    }
                    break;

                }
            }
            #[cfg(all(not(feature = "txn_clock")))]
            tx.send((num, total)).unwrap();
            // #[cfg(feature = "clock")]
            // tx.send((
            //     num,
            //     total,
            //     txn.timer.get_as_ms(0),
            //     txn.timer.get_as_ms(1),
            //     txn.timer.get_as_ms(2),
            //     txn.timer.get_as_ms(3),
            //     txn.timer.sample,
            // ))
            // .unwrap();
            // println!("txn {} commit {} of {}", i, num, total);
            #[cfg(feature = "txn_clock")]
            tx.send((num, total, timer.get_as_ms(2), timer.sample))
                .unwrap();
        });
    }

    let mut num = 0;
    let mut total = 0;
    let mut swapt = 0.0;
    let mut hit = 0.0;
    let mut read = 0.0;
    let mut update = 0.0;
    let mut txn = 0.0;
    let mut vec = Vec::<u128>::new();
    for i in 0..TEST_THREAD_COUNT {
        #[cfg(feature = "txn_clock")]
        {
            let (num0, total0, txn0, sample0) = rx.recv().unwrap();
            num += num0;
            total += total0;
            txn += txn0;
            vec.extend_from_slice(&sample0);
            vec.sort();
        }
        #[cfg(not(feature = "txn_clock"))]
        {
            let (num0, total0) = rx.recv().unwrap();
            num += num0;
            total += total0;
            println!("{}: {} of {} txns committed", i, num0, total0);
        }
    }
    #[cfg(all(not(feature = "clock"), not(feature = "txn_clock")))]
    println!(
        "total txn {} of {} txns committed per second",
        num / 10,
        total / 10
    );
    #[cfg(feature = "clock")]
    println!(
            "total txn {} of {} txns committed per second, avg_swap: {}, avg_hit: {}, avg_read: {:.3}, avg_update: {:.3}, 10%: {:3}, 90%: {:3}",
            num / 10,
            total / 10,
            swapt as f64 / TEST_THREAD_COUNT as f64,
            hit as f64 / TEST_THREAD_COUNT as f64,
            read as f64 / TEST_THREAD_COUNT as f64,
            update as f64 / TEST_THREAD_COUNT as f64,
            vec.get(THREAD_COUNT * 1000).unwrap(),
            vec.get(THREAD_COUNT * 9500).unwrap(),
        );
    #[cfg(feature = "txn_clock")]
    println!(
            "total txn {} of {} txns committed per second, avg_txn: {:.3}, 10%: {:3}, 95%: {:3}, 99%: {:3}",
            num / 30,
            total / 30,
            txn as f64 / TEST_THREAD_COUNT as f64,
            vec.get(TEST_THREAD_COUNT * 1000).unwrap(),
            vec.get(TEST_THREAD_COUNT * 9500).unwrap(),
            vec.get(TEST_THREAD_COUNT * 9900).unwrap(),
        );
    // #[cfg(feature = "clock")]
    // for v in vec {
    //     f.write(v.to_string().as_bytes()).unwrap();
    //     f.write(b"\n").unwrap();
    // }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tpcc_test_sync() {
        // log4rs::init_file("log4rs.yaml", Default::default()).unwrap();
        // debug!("INFO");
        // debug!("DEBUG");
        tpcc_test();
    }

    // #[test]
    // fn tpcc_test_async() {
    //     NVMTableStorage::init_test_database();
    //     Catalog::init_catalog();
    //     if IS_FULL_SCHEMA {
    //         tpcc_init::init_schema("config/schema_file/TPCC_full_schema.txt");
    //     } else {
    //         tpcc_init::init_schema("config/schema_file/TPCC_short_schema.txt");
    //     }
    //     init_tables();
    //     // let threads::Vec<JoinHandle> = Vec::new();

    //     let (tx0, rx) = mpsc::channel();
    //     // let senders = Vec::new();
    //     println!("test start");

    //     let catalog = Catalog::global();
    //     let total_time = Duration::new(10, 0);
    //     let tpthread = TRANSACTION_COUNT / THREAD_COUNT;
    //     for i in 0..THREAD_COUNT {
    //         let tx1 = tx0.clone();
    //         thread::spawn(move || {
    //             let (executor, spawner) = Executor::new();
    //             for t_cnt in (i * tpthread)..(i * tpthread + tpthread) {
    //                 let tx = tx1.clone();
    //                 spawner.spawn(async move {
    //                     let mut buffer = TransactionBuffer::new(catalog, t_cnt as u64);
    //                     println!("test start {} {}", t_cnt, buffer.address);
    //                     let mut rng = rand::thread_rng();

    //                     let mut txn = Transaction::new(&mut buffer, false);
    //                     let tablelist = TableList::new(&catalog);

    //                     let mut num = 0;
    //                     let mut total = 0;
    //                     let start = SystemTime::now();
    //                     loop {

    //                         total = total + 1;
    //                         let mut commited: bool = false;
    //                         if u64_rand(&mut rng, 1, 88) > 45 {
    //                             let query = TpccQuery::gen_payment(&mut rng, t_cnt as u64);
    //                             commited =
    //                                 tpcc_txn_asycn::run_payment(&mut txn, &tablelist, &query).await;
    //                         } else {
    //                             let query = TpccQuery::gen_new_order(&mut rng, t_cnt as u64);

    //                             commited =
    //                                 tpcc_txn_asycn::run_new_order(&mut txn, &tablelist, &query)
    //                                     .await;
    //                         }
    //                         if commited {
    //                             num = num + 1;
    //                         }

    //                         let end = SystemTime::now();
    //                         if end.duration_since(start).unwrap().ge(&total_time) {
    //                             break;
    //                         }
    //                     }
    //                     tx.send((num, total)).unwrap();
    //                 });
    //             }

    //             drop(spawner);
    //             executor.run();
    //         });
    //     }
    //     let mut num = 0;
    //     let mut total = 0;
    //     for i in 0..TRANSACTION_COUNT {
    //         let (num0, total0) = rx.recv().unwrap();
    //         num += num0;
    //         total += total0;
    //         println!("{}: {} of {} txns committed", i, num0, total0);
    //     }
    //     println!(
    //         "total new order {} of {} txns committed",
    //         num / 10,
    //         total / 10
    //     );
    // }
}
