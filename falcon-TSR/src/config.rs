use std::mem::size_of;

use crate::{mvcc_config::{TEST_THREAD_COUNT_BASE, THREAD_COUNT}, storage::row::TUPLE_HEADER};

pub const NVM_ADDR: u64 = 0x1_000_000_000;
pub static mut CATALOG_ADDRESS: u64 = NVM_ADDR + PAGE_SIZE;
// pub const CLWB_SIZE: u64 = 64;
#[cfg(feature = "native")]
pub const MAX_PAGE_COUNT: u64 = 16000;
#[cfg(feature = "native")]
pub const PAGE_SIZE: u64 = 0x20000; // 800000
#[cfg(feature = "nvm_server")]
pub const MAX_PAGE_COUNT: u64 = 200000;
#[cfg(feature = "nvm_server")]
pub const PAGE_SIZE: u64 = 0x200000; // 800000
pub const U64_OFFSET: u64 = 8;
pub const USIZE_OFFSET: u64 = size_of::<usize>() as u64;
pub type Address = u64;

#[cfg(not(feature = "tpcc"))]
pub const TUPLE_SIZE: usize = 1056;
#[cfg(feature = "tpcc")]
pub const TUPLE_SIZE: usize = 512;

pub const POOL_SIZE: usize = 1 * 1024 * 1024;
pub const POOL_PERC: usize = 1;
pub const ADDRESS_MASK: u64 = (1u64 << 48) - 1;
pub const POW_2_63: u64 = 1u64 << 63;
pub static mut BATCH_SIZE: u64 = 16;

// #[cfg(feature = "single_writer")]
// pub const TEST_THREAD_COUNT: usize = TEST_THREAD_COUNT_BASE / 2;
// #[cfg(not(feature = "single_writer"))]
pub const TEST_THREAD_COUNT: usize = crate::mvcc_config::TEST_THREAD_COUNT_BASE;

pub const TRANSACTION_COUNT: usize = TEST_THREAD_COUNT_BASE;
pub const YCSB_SIZE: usize = 1024;
pub const CAS_LATENCY: u32 = 2000;