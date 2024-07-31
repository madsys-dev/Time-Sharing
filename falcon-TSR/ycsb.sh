rm /mnt/pmem0/pmem_hash.data
cd dash
git checkout ycsb
cd ..
taskset -c 0-20 cargo test ycsb_test_sync --release -- --nocapture
taskset -c 0-20 cargo run --release -- ycsb 1.0
