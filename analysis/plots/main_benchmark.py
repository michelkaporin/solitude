import numpy as np
import os
import re
import math

import sqlite3
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages


###############
### Parsing ###
###############
baseline_retrieval = [] # HyperDex | S3 | Cassandra
baseline_ddd = [] # HyperDex | S3 | Cassandra
baseline_search = [] # HyperDex | S3 | Cassandra

stream_retrieval = []
stream_ddd = []
stream_search = []

def load_data(filename):
    with open(filename, 'r') as file:
        for i in range(4): next(file) # skip header
        
        baseline = True 
        b_retrieval = []; b_ddd = []; b_search = []
        s_retrieval = []; s_ddd = []; s_search = []

        for line in file:
            if line.rstrip() == 'BASELINE': 
                baseline = True 
                for i in range(7): next(file) 
                continue
            if line.rstrip() == 'LABELLED DESIGN': 
                baseline = False
                for i in range(6): next(file) # skip header
                continue
            else:
                line = line.rstrip()
                retrieval = float(line.split("\t")[3])
                ddd = float(line.split("\t")[4])
                search = float(line.split("\t")[5])

                if baseline:
                    b_retrieval.append(retrieval)
                    b_ddd.append(ddd)
                    b_search.append(search)
                else:
                    s_retrieval.append(retrieval)
                    s_ddd.append(ddd)
                    s_search.append(search)

                if len(b_retrieval) == 3 and len(b_ddd) == 3 and len(b_search) == 3:
                    baseline_retrieval.append(b_retrieval); b_retrieval = []
                    baseline_ddd.append(b_ddd); b_ddd = []
                    baseline_search.append(b_search); b_search = []
                if len(s_retrieval) == 3 and len(s_ddd) == 3 and len(s_search) == 3:
                    stream_retrieval.append(s_retrieval); s_retrieval = []
                    stream_ddd.append(s_ddd); s_ddd = []
                    stream_search.append(s_search); s_search = []

def compute_avg_std(data):
    return np.average(data, axis=0), np.std(data, axis=0)
    
load_data('../thesis_prepared_raw_data/main_benchmark.log')

#########
#  PLOT #
#########

golden_mean = ((math.sqrt(5) - 1.0) / 2.0) * 0.8
fig_with_pt = 600
inches_per_pt = 1.0 / 72.27 * 2
fig_with = fig_with_pt * inches_per_pt
fig_height = fig_with * golden_mean
fig_size = [fig_height, fig_height]

params = {'backend': 'ps',
          'axes.labelsize': 22,
          'font.size': 22,
          'legend.fontsize': 20,
          'xtick.labelsize': 20,
          'ytick.labelsize': 20,
          'figure.figsize': fig_size,
          'font.family': 'Times New Roman'}

# plot_latency fixed
plt.rcParams.update(params)
plt.rc('pdf', fonttype=42)

# Barplot
f, (ax1, ax3) = plt.subplots(2, 1, sharex=True)
types = ['HyperDex', 'Amazon S3', 'Cassandra']
mean_baseline_retrieval, std_baseline_retrieval = compute_avg_std(baseline_retrieval)#[1, 1, 1], [0.1, 0.2, 1] #[HyperDex, S3, Cassandra],[HyperDex, S3, Cassandra]
mean_baseline_ddd, std_baseline_ddd = compute_avg_std(baseline_ddd)
mean_baseline_search, std_baseline_search = compute_avg_std(baseline_search)
mean_stream_retrieval, std_stream_retrieval = compute_avg_std(stream_retrieval)#[1, 1, 1], [0.1, 0.2, 1] #[HyperDex, S3, Cassandra],[HyperDex, S3, Cassandra]
mean_stream_ddd, std_stream_ddd = compute_avg_std(stream_ddd)
mean_stream_search, std_stream_search = compute_avg_std(stream_search)
ind = np.arange(len(types))
width = 0.5

rects1 = ax1.bar(ind, mean_baseline_retrieval, width, color='0.25', yerr=std_baseline_retrieval, align='center', error_kw=dict(ecolor='0.75', lw=2, capsize=5, capthick=2))
rects2 = ax1.bar(ind, mean_baseline_ddd, width, color='0.50', bottom=mean_baseline_retrieval, yerr=std_baseline_ddd, align='center', error_kw=dict(ecolor='0.00', lw=2, capsize=5, capthick=2))
rects3 = ax1.bar(ind, mean_baseline_search, width, color='0.75', bottom=mean_baseline_retrieval, yerr=std_baseline_search, align='center', error_kw=dict(ecolor='0.25', lw=2, capsize=5, capthick=2))

ax3.bar(ind, mean_baseline_retrieval, width, color='0.25', yerr=std_baseline_retrieval, align='center', error_kw=dict(ecolor='0.8', lw=2, capsize=5, capthick=2))
ax3.bar(ind, mean_baseline_ddd, width, color='0.50', bottom=mean_baseline_retrieval, yerr=std_baseline_ddd, align='center', error_kw=dict(ecolor='0.00', lw=2, capsize=5, capthick=2))
ax3.bar(ind, mean_baseline_search, width, color='0.75', bottom=mean_baseline_retrieval, yerr=std_baseline_search, align='center', error_kw=dict(ecolor='0.25', lw=2, capsize=5, capthick=2))

ax1.set_ylim(2500, 3500)  # outliers only
ax3.set_ylim(0, 500)  # most of the data

# hide the spines between ax and ax2
ax1.spines['bottom'].set_visible(False)
ax3.spines['top'].set_visible(False)
ax1.xaxis.tick_top()
ax1.tick_params(labeltop='off')  # don't put tick labels at the top
ax3.xaxis.tick_bottom()
d = .015  # how big to make the diagonal lines in axes coordinates
# arguments to pass to plot, just so we don't keep repeating them
kwargs = dict(transform=ax1.transAxes, color='k', clip_on=False)
ax1.plot((-d, +d), (-d, +d), **kwargs)        # top-left diagonal
ax1.plot((1 - d, 1 + d), (-d, +d), **kwargs)  # top-right diagonal

kwargs.update(transform=ax3.transAxes)  # switch to the bottom axes
ax3.plot((-d, +d), (1 - d, 1 + d), **kwargs)  # bottom-left diagonal
ax3.plot((1 - d, 1 + d), (1 - d, 1 + d), **kwargs)  # bottom-right diagonal

plt.xticks(ind, types)
# ax3.set_xlabel("Baseline design")
ax1.grid(True, linestyle=':', color='0.8', zorder=0)
ax3.grid(True, linestyle=':', color='0.8', zorder=0)
ax1.legend((rects1[0], rects2[0], rects3[0]), ('Retrieval', 'Decoding', 'Search'), loc="upper right")
f.text(0.05, 0.5, 'Time [ms]', va='center', rotation='vertical')
plt.subplots_adjust(left=0.175)
#plt.axis('tight')

F = plt.gcf()
F.set_size_inches(fig_size)
pdf_pages = PdfPages("../plots/images/main_benchmark1.pdf")
pdf_pages.savefig(F, bbox_inches='tight')
plt.clf()
pdf_pages.close()

### 
### SECOND PLOT
###
f, ax2 = plt.subplots()
ax2.bar(ind, mean_stream_retrieval, width, color='0.25', yerr=std_stream_retrieval, align='center', error_kw=dict(ecolor='0.8', lw=2, capsize=5, capthick=2))
ax2.bar(ind, mean_stream_ddd, width, color='0.50', bottom=mean_stream_retrieval, align='center', yerr=std_stream_ddd, error_kw=dict(ecolor='0.00', lw=2, capsize=5, capthick=2))
ax2.bar(ind, mean_stream_search, width, color='0.75', bottom=mean_stream_retrieval, align='center', yerr=std_stream_search, error_kw=dict(ecolor='0.25', lw=2, capsize=5, capthick=2))

#ax2.set_ylabel("Time [ms]")
plt.xticks(ind, types)
# ax2.set_xlabel("Stream design")
ax2.grid(True, linestyle=':', color='0.8', zorder=0)
#plt.axis('tight')

F = plt.gcf()
F.set_size_inches(fig_size)
pdf_pages = PdfPages("../plots/images/main_benchmark2.pdf")
pdf_pages.savefig(F, bbox_inches='tight')
plt.clf()
pdf_pages.close()