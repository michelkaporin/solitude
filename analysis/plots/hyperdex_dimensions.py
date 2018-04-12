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
c = []
cc = []
ccc = []
def load_data(filename):
    with open(filename, 'r') as file:
        next(file) # skip header
        for line in file:
            if not line.strip():
                continue
            elif line.startswith('Chunked,'): 
                arr = c; continue
            elif line.startswith('ChunkedCompressed,'): 
                arr = cc; continue
            elif line.startswith('ChunkedCompressedEncrypted,'):
                arr = ccc; continue
            else: # values
                vals = [float(line.rstrip())]
                for i in xrange(2):
                    vals.append(float(next(file).rstrip()))
                arr.append(vals)

def compute_avg_std(data):
    return np.average(data, axis=1), np.std(data, axis=1)
    
load_data('data/hyperdex_dimensions.data')

#########
#  PLOT #
#########

golden_mean = ((math.sqrt(5) - 1.0) / 2.0) * 0.8
fig_with_pt = 400
inches_per_pt = 1.0 / 72.27 * 2
fig_with = fig_with_pt * inches_per_pt
fig_height = fig_with * golden_mean
fig_size = [fig_with, fig_height / 1.22]

params = {'backend': 'ps',
          'axes.labelsize': 20,
          'legend.fontsize': 18,
          'xtick.labelsize': 18,
          'ytick.labelsize': 18,
          'font.size': 18,
          'figure.figsize': fig_size,
          'font.family': 'times new roman'}


# plot_latency fixed
plt.rcParams.update(params)
plt.rc('pdf', fonttype=42)

# Barplot
f, ax1 = plt.subplots()
types = ['n=1, single', 'n=1, two', 'n=1000, single', 'n=1000, two']
mean_c, std_c = compute_avg_std(c)
mean_cc, std_cc = compute_avg_std(cc)
mean_ccc, std_ccc = compute_avg_std(ccc)
ind = np.arange(len(types))
width = 0.27

rects1 = ax1.bar(ind, mean_c, width, color='0.25', yerr=std_c, error_kw=dict(ecolor='0.75', lw=2, capsize=5, capthick=2))
rects2 = ax1.bar(ind + width, mean_cc, width, color='0.50', yerr=std_cc, error_kw=dict(ecolor='0.00', lw=2, capsize=5, capthick=2))
rects3 = ax1.bar(ind + 2*width, mean_ccc, width, color='0.75', yerr=std_ccc, error_kw=dict(ecolor='0.25', lw=2, capsize=5, capthick=2))

ax1.set_ylabel("Time [ms]")
ax1.set_xticks(ind + width)
ax1.set_xticklabels(types)
ax1.set_xlabel("Block size n, dimensionality")
ax1.grid(True, linestyle=':', color='0.8', zorder=0)

ax1.legend((rects1[0], rects2[0], rects3[0]), ('Chunked', 'Chunked, compressed', 'Chunked, compressed, encrypted'), loc="upper left")

plt.axis('tight')

F = plt.gcf()
F.set_size_inches(fig_size)
pdf_pages = PdfPages("../plots/images/hyperdex_dimensions.pdf")
pdf_pages.savefig(F, bbox_inches='tight')
plt.clf()
pdf_pages.close()