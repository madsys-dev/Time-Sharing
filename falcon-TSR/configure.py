numa_set = ""
db_file_path = "_test_persist"
pm_index = "pmem_hash.data"
pm_btree_index = "pmem_btree.data"
TPCC_WAREHOUSE = 64
YCSB_TOTAL = "16 * 1024 * 1024"

if __name__ == "__main__":
    with open('tpcc.sh', "w") as script:
        script.write("rm %s\n"%pm_index)
        script.write("cd dash\n")
        script.write("git checkout tpcc\n")
        script.write("cd ..\n")
        script.write(numa_set + " cargo test tpcc_test_sync --release -- --nocapture\n")

    with open('ycsb.sh', "w") as script:
        script.write("rm %s\n"%pm_index)
        script.write("cd dash\n")
        script.write("git checkout ycsb\n")
        script.write("cd ..\n")
        script.write(numa_set + " cargo test ycsb_test_sync --release -- --nocapture\n")

    with open('restore.sh', "w") as script:
        script.write(numa_set + " cargo test ycsb_test_reload --release -- --nocapture\n")

    with open('src/customer_config.rs', "w") as rust_code:
        rust_code.write('pub const TPCC_WAREHOUSE: u64 = %d;\n'%TPCC_WAREHOUSE)
        rust_code.write('pub const YCSB_TOTAL: u64 = %s;\n'%YCSB_TOTAL)
        rust_code.write('pub const NVM_FILE_PATH: &str = "%s";\n'%db_file_path)
        rust_code.write('pub const INDEX_FILE_PATH: &str = "%s";\n'%pm_index)
        rust_code.write('pub const BTREE_FILE_PATH: &str = "%s";\n'%pm_btree_index)

        

