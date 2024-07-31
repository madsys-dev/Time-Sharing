use libc::c_void;
use n2db::config::BATCH_SIZE;
use n2db::replica::local_table::LocalTable;
use n2db::storage::allocator::{self, LocalBufferAllocator};
use n2db::storage::nvm_file::NVMTableStorage;
use n2db::storage::row::Tuple;
use n2db::storage::schema::{ColumnType, TableSchema};
use n2db::storage::table::TupleId;
use n2db::storage::timestamp::TimeStamp;
// use n2db::tpcc::u64_rand;
// use n2db::utils::{file, io};
// use std::ptr;
#[cfg(feature = "nbtree")]
use n2db::storage::index::nbtree::NBTree;
use n2db::tpcc::tpcc_test::tpcc_test;
use n2db::utils::file::{numa_alloc_onnode, numa_free, sfence};
use n2db::utils::io::nt_load;
use n2db::ycsb::ycsb_test::ycsb_test;
use n2db::ycsb::{ycsb_test, Properties, YCSBWorkload};
use std::sync::atomic::AtomicU64;
use std::sync::{mpsc, Arc, Barrier};
use std::time::{Duration, Instant, SystemTime};
use std::{env, thread, time};
// use n2db::storage::index::dash::Dash;
// use n2db::storage::index::dashstring::DashString;
// use chrono::prelude::*;
// use n2db::c::ffi::{init, plus};
#[cfg(feature = "rust_map")]
pub fn test_bztree() {
    use bztree::BzTree;

    let tree = BzTree::<u64, u64>::default();
    let guard = crossbeam_epoch::pin();

    assert_eq!(tree.upsert(3, 1, &guard), None);
    assert_eq!(tree.upsert(3, 5, &guard), Some(&1));
    tree.insert(6, 10, &guard);
    tree.insert(9, 12, &guard);

    // assert!(matches!(tree.delete(&key1, &guard), Some(&10)));

    // let key2 = "key_2".to_string();
    // tree.insert(key2.clone(), 2, &guard);
    // assert!(tree.compute(&key2, |(_, v)| Some(v + 1), &guard));
    // assert!(matches!(tree.get(&key2, &guard), Some(&3)));

    // assert!(tree.compute(&key2, |(_, v)| {
    // if *v == 3 {
    //     None
    // } else {
    //     Some(v + 1)
    // }
    // }, &guard));
    // assert!(matches!(tree.get(&key2, &guard), None));

    let r1 = tree.range(2..8, &guard).last();
    println!("{:?}", r1);

    println!("{:?}", tree.first(&guard));
    tree.pop_first(&guard);
    println!("{:?}", tree.first(&guard));
    tree.pop_first(&guard);
    println!("{:?}", tree.first(&guard));
}
#[cfg(feature = "nbtree")]
fn test_nbtree() {
    NVMTableStorage::init_test_database();
    let tree = NBTree::<u64>::new();
    let index = Arc::new(tree);
    let barrier = Arc::new(Barrier::new(1));
    // let mut handles = Vec::with_capacity(10);

    // let t1 = index.clone();
    // let b1 = barrier.clone();
    // handles.push(thread::spawn(move || {
    //     crate::storage::index::nbtree::init_index(0);
    //     b1.wait();
    //     for i in 0..100 {
    //         t1.insert(i, TupleId{page_start: AtomicU64::new(i)});
    //     }
    //     b1.wait();
    //     println!("{:?}", t1.range(&18, &23));

    //     println!("{}", t1.get(&15).unwrap().get_address());
    //     println!("finish1");

    // }));
    // let t2 = index.clone();
    // let b2 = barrier.clone();
    // handles.push(thread::spawn(move || {
    //     crate::storage::index::nbtree::init_index(2);
    //     b2.wait();
    //     for i in 100..200 {
    //         t2.insert(i, TupleId{page_start: AtomicU64::new(i)});

    //     }
    //     println!("{:?}", t2.range(&104, &109));

    //     b2.wait();

    //     println!("{}", t2.get(&115).unwrap().get_address());
    //     println!("finish2");

    // }));
    // for handle in handles {
    //     handle.join().unwrap();
    // }
    // println!("finish");
    let t = index.clone();
    n2db::storage::index::nbtree::init_index(1);
    println!("{}", t.get(&10115).unwrap().get_address());

    for i in 200..300 {
        t.insert(
            i,
            TupleId {
                page_start: AtomicU64::new(i),
            },
        );
    }
    // println!("insert");
    println!("{:?}", t.range(&195, &205));
    println!("{:?}", t.last(&195, &205));
    println!("{:?}", t.last(&288, &305));
    println!("{:?}", t.last(&300, &305));

    // println!("{}", t.get(&101).unwrap().get_address());
}
fn test_local_allocator() {
    let mut allocator = LocalBufferAllocator::new(0, 100);
    let tid1 = allocator.allocate_tuple(100).unwrap();
    let tid2 = allocator.allocate_tuple(100).unwrap();
    let tid3 = allocator.allocate_tuple(100).unwrap();

    let mut schema = TableSchema::new();
    schema.push(ColumnType::Int64, "a");
    schema.push(ColumnType::Int64, "b");
    let tuple = Tuple::new(tid1.get_address(), "1,2", &schema, TimeStamp::default()).unwrap();
    println!("{:?}", tid1);
    println!("{:?}", tid2);
    println!("{:?}", tid3);
    println!(
        "{:?}",
        tuple.get_data_by_column(schema.get_column_offset(1))
    );
}
fn massage_test() {
    unsafe {
        let address = numa_alloc_onnode(4096, 2) as u64;
        let barrier = Arc::new(Barrier::new(2));
        let b1 = barrier.clone();
        let b2 = barrier.clone();

        let t1 = thread::spawn(move || {
            b1.wait();
            let s1 = nt_load(address as *mut c_void, 0);
            let s2 = nt_load(address as *mut c_void, 1);
            println!("read {}, {}", s1, s2);
            b1.wait();
            let a = address as *mut u64;
            *a = 100;
            let b = (address as u64 + 8) as *mut u64;
            *b = 200;
            // sfence();
            // println!("{}", address as u64);
            b1.wait();
            // let s1 = nt_load(address as *mut c_void, 0);
            // let s2 = nt_load(address as *mut c_void, 1);
            // println!("read {}, {}", s1, s2);
        });
        let t2 = thread::spawn(move || {
            b2.wait();
            let start: SystemTime = SystemTime::now();
            // let s1: u64 = n2db::utils::io::read(address);
            // let s2: u64 = n2db::utils::io::read(address + 8);
            let s1 = nt_load(address as *mut c_void, 0);
            let s2 = nt_load(address as *mut c_void, 1);
            let end = SystemTime::now();
            println!("during: {:?}", end.duration_since(start).unwrap());
            println!("read2 {}, {}", s1, s2);
            b2.wait();
            b2.wait();
            let start = SystemTime::now();
            let s1 = nt_load(address as *mut c_void, 0);
            let s2 = nt_load(address as *mut c_void, 1);

            // let s1: u64 = n2db::utils::io::read(address);
            // let s2: u64 = n2db::utils::io::read(address + 8);
            let end = SystemTime::now();
            println!("during: {:?}", end.duration_since(start).unwrap());
            // let s1 = *a;
            // let b = (address as u64 + 8) as *mut u64;
            // let s2 = *b;
            println!("read2 {}, {}", s1, s2);
        });
        t1.join().unwrap();
        t2.join().unwrap();
        numa_free(address as *mut c_void, 4096);
    }
    for i in 1..100 {
        let start = Instant::now();
        n2db::utils::wait_for_nanos(i * 100);
        let end = Instant::now();
        println!("{}, during: {:?}", i, end.duration_since(start));
    }
}
fn main() {
    let args: Vec<String> = env::args().collect();
    let query = &args[1];
    if query.eq("ycsb") {
        let mut props = Properties::default();
        if args.len() > 2 {
            let read_perc = str::parse::<f64>(&args[2]).unwrap();
            props.workload.read_perc = read_perc;
            props.workload.write_perc = 1.0 - read_perc;
        }
        if args.len() > 3 {
            unsafe { BATCH_SIZE = str::parse::<u64>(&args[3]).unwrap() }
        }

        dbg!(props);
        unsafe { dbg!(BATCH_SIZE) };
        ycsb_test(props);
    }
    if query.eq("tpcc") {
        if args.len() > 2 {
            unsafe { BATCH_SIZE = str::parse::<u64>(&args[2]).unwrap() }
        }
        unsafe { dbg!(BATCH_SIZE) };
        tpcc_test();
    }
}
