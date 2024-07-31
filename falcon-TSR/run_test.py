
# cargo test test_isolation --features basic --features local_cc_cfg_occ --no-default-features --release -- --nocapture
import os
import copy
import re
import sys
import getopt
from configure import numa_set, pm_index

k = 0
index_type = "NVM"
env = "cxl"
mink = 0
maxk = 999
batch_size = 16

sysname = {
    "TSR": ["cxl_db", env, "time_sharding", "undo_log"],
    "SW": ["cxl_db", env, "single_writer", "undo_log"],
    "MW": ["cxl_db", env, "multi_writer", "undo_log"],

    "Unknown": []
}

def copy(dir, file):
    cmd = 'cp ' + file + ' ' + os.path.join(dir, file)
    print(cmd)
    os.system(cmd)

def set_thread_count(thread_count, ycsb_size = 2048):
    path = './src/mvcc_config/mod.rs'
    # print(thread_count)
    with open(path, "w") as cfile:
        cfile.write("pub mod delta;\n")
        cfile.write("pub const THREAD_COUNT: usize = 16;\n")
        cfile.write("pub const TEST_THREAD_COUNT_BASE: usize = %d;\n" % (thread_count))
        # cfile.write("pub const TRANSACTION_COUNT: usize = THREAD_COUNT;\n")
        # cfile.write("pub const YCSB_SIZE:usize = %d;" % (ycsb_size))

def get_sysname(features):
    global index_type
    global sysname

    result = "Unknown"

    for key in sysname:
        if set(sysname[key]) < set(features) and len(sysname[key]) > len(sysname[result]):
            result = key
    if result == "Falcon" and index_type == "DRAM":
        result = "Falcon(DRAM Index)"
    return result

def get_workload(features, result):
    if "tpcc" in features:
        if "new_order_clock" in features:
            return "TPC-C-NP new_order"
        if "payment_clock" in features:
            return "TPC-C-NP payment"
        return "TPC-C-NP"
    for ycsb in "abcdef":
        workload = "ycsb_"+ycsb
        if workload in features:
            theta = re.findall("theta = (\d+\.?\d*)", result)
            return workload + " zipf_theta = " + str(theta[0])
    return "Unknown"

def get_cc(features):
    cc = ""
    if "mvcc" in features:
        cc = "MV"
    if "local_cc_cfg_2pl" in features:
        cc += '2PL'
    elif "local_cc_cfg_to" in features:
        cc += 'TO'
    elif "local_cc_cfg_occ" in features:
        cc += 'OCC'
    else:
        cc = "Unknown"
    return cc

def get_txn(result):
    pattern = "txn (\d+) of (\d+) txns"
    txns = re.findall(pattern, result)
    return txns[0]

def get_latency(result):
    pattern = "wait: (\d+\.?\d*)us, execute: (\d+\.?\d*)us, apply: (\d+\.?\d*)us"
    latency_group = re.findall(pattern, result)
    if latency_group:
        return latency_group[0]
    return []

# "sysname, workload, threads, cc, commit txns, total txns, avg latency, 10% latency, 95% latency. 99% latency"
def report(features, result, t_cnt, args = ""):
    csv_line = [args]
    csv_line.append(get_sysname(features))
    csv_line.append(get_workload(features, result))
    csv_line.append(str(t_cnt))
    csv_line.append(get_cc(features))

    commits, total = get_txn(result)
    csv_line.append(str(commits))
    csv_line.append(str(total))

    latency_group = get_latency(result)
    for latency in latency_group:
        csv_line.append(str(latency))
    return ",".join(csv_line) + "\n"


def TIMEOUT_COMMAND(command, timeout):
    """call shell-commpand and either return its output or kill it
    if it doesn't normally exit within timeout seconds and return None"""
    import subprocess, datetime, os, time, signal
    start = datetime.datetime.now()
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    while process.poll() is None:
        time.sleep(0.2)
        now = datetime.datetime.now()
        if (now - start).seconds > timeout:
            os.kill(process.pid)
            os.waitpid(-1, os.WNOHANG)
            return None
    return process.stdout.read().decode("utf-8")

def run_test(features, workload, result_path, args = ""):
    global k, mink, maxk
    # set_thread_count(16)

    k += 1
    # test tasks filter
    # if k > 22 or "n2db_local" in features:
    #     return ""
    if k < mink or k > maxk:
        return ""
    # if "multi_writer" not in features:
    #     return ""
    # if 32 not in features:
    #     return ""
    # if "tpcc" in features and "hot_unflush" not in features:
    #     return ""

    result_csv = ""

    # txt = "taskset -c 1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43,45,47,49,51,53,55,57,59,61,63,65,67,69,71,73,75,77,79,81,83,85,87,89,91,93,95,97,99 cargo test "  + workload
    txt = numa_set + " cargo run " 
    t_cnt = 16
    ycsb_size = 2048
    for f in features:
        if f == "":
            continue
        if type(f) == int:
            if f <= 256:
                t_cnt = f
                continue
            else:
                ycsb_size = f
                continue
        txt += " --features " + f
        # result_csv += f + " "
    set_thread_count(t_cnt, ycsb_size)

    txt += " --no-default-features --release -- " + args
    print("test cmd: " + txt)
    text = ""
    # r = os.popen("rm %s"%pm_index)
    r = os.popen(txt)
    # text = TIMEOUT_COMMAND(txt, 600)
    # if text == None:
    #     with open("error.txt", "w") as err_file:
    #         err_file.write(result_path)
    #         err_file.write('\n')
    #         err_file.write(str(k))
    #         err_file.write('\n')
    #         err_file.write(txt)
    #         err_file.write('\n')
    #     r.close()
    #     return ""
    text = r.read()
    r.close()
    print(features)
    
    features.remove(t_cnt)
    features.append(str(t_cnt))
    if ycsb_size not in features:
        features.append(ycsb_size)
    features.remove(ycsb_size)
    features.append(str(ycsb_size))
    # print(features)
    try:
        os.mkdir("result/")
    except:
        pass
    try:
        os.mkdir("result/" + result_path)
    except:
        pass
    of = open("result/" + result_path + "/" + str(k) + ".txt", "w")
    of.write(text)
    of.close()
    # print("result/" + result_path + "/" + str(k) + ".txt")
    for line in text.splitlines():
        if "txns committed per second" in line:
            result_csv += report(features, line, t_cnt, args)
    
    
    of = open("result/" + result_path + "/"+ str(k) + ".csv", "w")
    of.write(result_csv)
    of.close()

    features.remove(str(t_cnt))
    features.append(t_cnt)

    features.remove(str(ycsb_size))
    features.append(ycsb_size)
    return result_csv

def test(features, props, prop_list, workload, result_path, times = 1):
    
    if len(prop_list) == 0:
        for i in range(0, times):
            if 'ycsb' in workload:
                for rp in [0.5, 5, 9.5]:#range(0, 11):
                    run_test(features, workload, result_path, gen_args(workload, rp)) 
                    # exit(0)
            else:
                run_test(features, workload, result_path, gen_args(workload, 0)) 
        # print(features)
        return

    for f in props[prop_list[0]]:
        features += f
        test(features, props, prop_list[1:], workload, result_path, times)
        for item in f:
            features.remove(item)

def gen_args(workload, rp):
    if 'ycsb' in workload:
        return "ycsb " + str(rp * 1.0 / 10.0) + " " + str(batch_size)
    return "tpcc " + str(batch_size)
# ycsb for ycsb-NVM
# tpcc for tpcc-NVM, support multi index without recovery
# dram for DRAM index
def set_index(index_type):
    pwd = os.getcwd()
    dash_owd = pwd + "/dash/src"
    os.chdir(dash_owd) 
    r = os.popen("git checkout " + index_type)
    os.chdir(pwd) 

def test_ycsb_cxl():
    global k
    global index_type
    k = 0
    index_type = "DRAM"
    # set_index("ycsb")

    props = {
        "basic": [["basic_dram"]],
        "ycsb": [
            ["ycsb_a"],
        ],
        "cc_cfg": [
            # ["local_cc_cfg_to"],
            ["local_cc_cfg_2pl"],
            # ["local_cc_cfg_occ"],
        ],  
        "mvcc": [
            [""],
            # ["mvcc"]
        ],
        "buffer": [
            # sysname["TSR"], 
            sysname["SW"], 
            # sysname["MW"], 
        ],
        "clock": [
            ["txn_clock"],
        ],
        "thread_count": [[16]]
    }
    test([], props, list(props.keys()), "ycsb_test_sync", "ycsb_tsm"+ str(batch_size))


def test_tpcc_cxl():
    global k
    global index_type
    k = 0
    index_type = "DRAM"
    # set_index("ycsb")

    props = {
        "basic": [["basic_dram", "tpcc"]],
        "ycsb": [
            ["ycsb_a"],
        ],
        "cc_cfg": [
            # ["local_cc_cfg_to"],
            # ["local_cc_cfg_2pl"],
            ["local_cc_cfg_occ"],
        ],  
        "mvcc": [
            [""],
            # ["mvcc"]
        ],
        "buffer": [
            # sysname["TSR"], 
            sysname["SW"], 
            # sysname["MW"], 
        ],
        "clock": [
            ["txn_clock"],
        ],
        # "thread_count": [[2], [4], [8],[16],[24],[32],[48],[64]]
        "thread_count": [[1]]


    }
    test([], props, list(props.keys()), "tpcc_test_sync", "tpcc_tsm"+ str(batch_size))

if __name__ == "__main__":

    if len(sys.argv) > 1:
        opts, args = getopt.getopt(sys.argv[1:], "c:b:", ['case', 'batch'])
        for opt, arg in opts:
            if opt in ['-c', '--case']:
                p = arg.split('-')
                try:
                    mink = int(p[0])
                except:
                    mink = 0
                try:
                    maxk = int(p[1])
                except:
                    maxk = 999
            if opt in ['-b', '--batch']:
                try:
                    batch_size = int(arg)
                except:
                    pass
    test_ycsb_cxl()
    # test_tpcc_cxl()

