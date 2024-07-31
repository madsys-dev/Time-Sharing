import os

header = r"read_perc,sysname,workload,threads,cc,commit txns,total txns,avg latency,10% latency,95% latency,99% latency"
sysnames = ["TSR", "SW", "MW"]
total_csv = header + "\n"

def collect(tasks):
    global total_csv
    results = []
    try:
        for task in tasks:
            total_csv += (task + "\n")
            cur_path = "result/" + task + "/"
            test_cases = os.listdir(cur_path)
            for test_case in range(0, 200):
                file_name = str(test_case) + ".csv"
                if file_name in test_cases:
                    with open(cur_path + file_name, "r") as result_csv:
                        for result in result_csv.readlines():
                            total_csv += result
                            results.append(result.strip().split(','))
    except:
        pass
    return results

def rename(name):
    new_name = ""
    for ycsb in "abcdef":
        workload = "ycsb_"+ycsb
        if workload in name:
            new_name += ycsb
    if "zipf_theta = 0.99" in name:
        new_name += "Z"
    elif "zipf_theta = 0" in name:
        new_name += "U"
    if "new_order" in name:
        new_name = "N"
    if "payment" in name:
        new_name = "P"
    return new_name

collect_csv = ""

for dir_name in ["ycsb_tsm30"]:

    results = collect([dir_name])
    collect_csv += dir_name + " \n"
    collect_csv += "workload,sysname,cc,2pl t0 commits txn,wait,execute,apply,t99 commits txn,wait,execute,apply,occ t0 commits txn,wait,execute,apply,t99 commits txn,wait,execute,apply\n"

    ycsb = {}
    workloads = []
    for cc in ['2PL', "OCC"]:
        for w in ["0", "0.99"]:
            for result in results:
                sysname = result[1]
                commits = result[6]
                workload = result[3] 
                if workload not in workloads:
                    workloads.append(workload)
                if not result[2].endswith(w):
                    continue
                if not result[4].endswith(cc):
                    continue
                ycsb.setdefault(workload, {})
                ycsb[workload].setdefault(sysname, [cc])
                ycsb[workload][sysname].append(commits)
                ycsb[workload][sysname].append(result[7])
                ycsb[workload][sysname].append(result[8])
                ycsb[workload][sysname].append(result[9])

    print(ycsb)
    print(workloads)

    for sysname in sysnames:
        # workloads = ycsb.keys()
        # workloads.sort()
        for workload in workloads:
            collect_csv += workload + "," + sysname
            try:
                collect_csv += "," + ",".join(ycsb[workload][sysname])
            except:
                pass
            collect_csv += "\n"

    collect_csv += "\n"

results = collect(["tpcc_tsm8"])

#collect_csv += dir_name + " \n"
collect_csv += "workload,sysname,commit\n"
print(results)
tpcc = {}
workloads = []
for result in results:
    sysname = result[1]
    commits = result[6]
    workload = result[0]
    if workload not in workloads:
        workloads.append(workload)
    tpcc.setdefault(workload, {})
    tpcc[workload].setdefault(sysname, [])
    tpcc[workload][sysname].append(commits)

print(tpcc)
for sysname in sysnames:
    for workload in workloads:
        collect_csv += workload + "," + sysname
        try:
            collect_csv += "," + ",".join(tpcc[workload][sysname])
        except:
            pass
        collect_csv += "\n"

# collect_csv += "\n"

with open("data.csv", "w") as output_csv:
    output_csv.write(total_csv)

with open("result.csv", "w") as output_csv:
    output_csv.write(collect_csv)