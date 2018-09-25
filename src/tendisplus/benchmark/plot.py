import matplotlib as mpl
import numpy as np
mpl.use('Agg')
import matplotlib.pyplot as plt
from scipy.interpolate import spline


titles = ["time", "cpu_usage", "mem", "readbytes", "writebytes", "read", "write", "storage_size", "imm_num", "flush_pending",
"compaction_pending", "size_active_memtable", "size_all_memtable", "estimate_table_read_mem"]

loads = ["load_100_1_40_4_64MB_1GB", "load_1_100_40_2_32MB_1GB", "load_1_100_40_4_64MB_1GB", "load_1_200_40_4_64MB_1GB"]

runs = ["run_100_1_40_4_64MB_1GB", "run_1_100_40_2_32MB_1GB", "run_1_100_40_4_64MB_1GB", "run_1_200_40_4_64MB_1GB"]

loads_seqs = []
runs_seqs = []
xticks = [i*60 for i in xrange(1600)]
for i in xrange(len(titles)):
    loads_seqs.append([])
    runs_seqs.append([])
    for load in (loads):
        fd = open(load, "r")
        lst = []
        for l in fd:
            eles = l.strip().split()
            assert(len(eles) == len(titles))
            lst.append(int(eles[i]))
        loads_seqs[i].append(lst)
    for run in (runs):
        fd = open(run, 'r')
        lst = []
        for l in fd:
            eles = l.strip().split()
            assert(len(eles) == len(titles))
            lst.append(int(eles[i]))
        runs_seqs[i].append(lst)

for i in xrange(len(loads)):
    tmp = loads_seqs[0][i][0]
    for j in xrange(len(loads_seqs[0][i])):
        loads_seqs[0][i][j] -= tmp
        if loads_seqs[0][i][j] < 0:
            loads_seqs[0][i][j] = loads_seqs[0][i][j-1]

for i in xrange(len(runs)):
    tmp = runs_seqs[0][i][0]
    for j in xrange(len(runs_seqs[0][i])):
        runs_seqs[0][i][j] -= tmp
        if runs_seqs[0][i][j] < 0:
            runs_seqs[0][i][j] = runs_seqs[0][i][j-1]

for i in xrange(len(titles)):
    if titles[i] == 'time': continue
    plt.figure(figsize=(16,12))
    for j in xrange(len(loads)):
        #time_seqs = loads_seqs[0][j]
        #xnew = np.linspace(time_seqs[0], time_seqs[-1], 300)
        #smooth = spline(loads_seqs[0][j], loads_seqs[i][j],xnew)
        #plt.plot(xnew,smooth, label = loads[j] + '_' + titles[i])
        plt.subplot(221+j)
        plt.xlabel("seconds")
        plt.plot(loads_seqs[0][j], loads_seqs[i][j])
        plt.title(loads[j] + '_' + titles[i])
    plt.suptitle(titles[i])
    #plt.legend() //with plt.plot.label arg
    plt.savefig("load_" + titles[i] + ".png")
    plt.close()
    plt.figure(figsize=(16,12))
    for j in xrange(len(runs)):
        #time_seqs = loads_seqs[0][j]
        #xnew = np.linspace(time_seqs[0], time_seqs[-1], 300)
        #smooth = spline(loads_seqs[0][j], loads_seqs[i][j],xnew)
        #plt.plot(xnew,smooth, label = loads[j] + '_' + titles[i])
        plt.subplot(221+j)
        plt.xlabel("seconds")
        plt.plot(runs_seqs[0][j], runs_seqs[i][j])
        plt.title(runs[j] + '_' + titles[i])
    plt.suptitle(titles[i])
    #plt.legend() //with plt.plot.label arg
    plt.savefig("run_" + titles[i] + ".png")
    plt.close()
