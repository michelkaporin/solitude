import numpy as np
import os
import re
import math

import sqlite3
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.ticker as ticker


###############
### Parsing ###
###############
labelled_res = []
ranged_res = []
def load_data(filename):
    labelled = False
    with open(filename, 'r') as file:
        for line in file:
            if line.startswith('Labelled data stored as'): 
                continue
            elif line.startswith('Labelled Design'): 
                labelled = True; continue
            else: # values
                vals = []
                for i in range(3):
                    val = float(line.split('\t')[1].rstrip())
                    vals.append(val)
                    try: line = next(file) 
                    except StopIteration: pass
                if labelled:
                    labelled_res.append(vals)
                    labelled = False
                else:
                    ranged_res.append(vals)

def compute_avg_std(data):
    return np.average(data, axis=1), np.std(data, axis=1)
    
load_data('data/hyperdex_labelled_design.data')
mean_labelled, std_labelled = compute_avg_std(labelled_res)
mean_ranged, std_ranged = compute_avg_std(ranged_res)

#########
#  PLOT #
#########

golden_mean = ((math.sqrt(5) - 1.0) / 2.0) * 0.8
fig_with_pt = 500
inches_per_pt = 1.0 / 72.27 * 2
fig_with = fig_with_pt * inches_per_pt
fig_height = fig_with * golden_mean
fig_size = [fig_with, fig_height]

params = {'backend': 'ps',
          'axes.labelsize': 20,
          'legend.fontsize': 18,
          'xtick.labelsize': 18,
          'ytick.labelsize': 18,
          'font.size': 18,
          'figure.figsize': fig_size,
          'font.family': 'times new roman'}

plt.rcParams.update(params)
plt.rc('pdf', fonttype=42)

# Barplot
f, (ax1, ax2) = plt.subplots(2, 1, sharex=True)
types = ['Low temperature range', 'Middle temperature range', 'High temperature range']
ind = np.arange(len(types))
width = 0.27

# Top
rects1 = ax1.bar(ind, mean_ranged, width, color='0.25', yerr=std_ranged, error_kw=dict(ecolor='0.75', lw=2, capsize=5, capthick=2))
rects2 = ax1.bar(ind + width, mean_labelled, width, color='0.50', yerr=std_labelled, error_kw=dict(ecolor='0.00', lw=2, capsize=5, capthick=2))

# Bottom
rects1 = ax2.bar(ind, mean_ranged, width, color='0.25', yerr=std_ranged, error_kw=dict(ecolor='0.75', lw=2, capsize=5, capthick=2))
rects2 = ax2.bar(ind + width, mean_labelled, width, color='0.50', yerr=std_labelled, error_kw=dict(ecolor='0.00', lw=2, capsize=5, capthick=2))

ax1.set_ylim(100, 150)  # outliers only
ax2.set_ylim(0, .15)  # most of the data
# hide the spines between ax and ax2
ax1.spines['bottom'].set_visible(False)
ax2.spines['top'].set_visible(False)
ax1.xaxis.tick_top()
ax1.tick_params(labeltop='off')  # don't put tick labels at the top
ax2.xaxis.tick_bottom()
d = .015  # how big to make the diagonal lines in axes coordinates
# arguments to pass to plot, just so we don't keep repeating them
kwargs = dict(transform=ax1.transAxes, color='k', clip_on=False)
ax1.plot((-d, +d), (-d, +d), **kwargs)        # top-left diagonal
ax1.plot((1 - d, 1 + d), (-d, +d), **kwargs)  # top-right diagonal

kwargs.update(transform=ax2.transAxes)  # switch to the bottom axes
ax2.plot((-d, +d), (1 - d, 1 + d), **kwargs)  # bottom-left diagonal
ax2.plot((1 - d, 1 + d), (1 - d, 1 + d), **kwargs)  # bottom-right diagonal


f.text(0.05, 0.5, 'Time [ms]', va='center', rotation='vertical')
ax2.yaxis.set_major_locator(ticker.MaxNLocator(5))

ax1.set_xticks(ind + width)
ax1.set_xticklabels(types)
ax1.grid(True, linestyle=':', color='0.8', zorder=0)
ax2.grid(True, linestyle=':', color='0.8', zorder=0)

ax1.legend((rects1[0], rects2[0]), ('Range query', 'Stream design'), loc="upper right")

#plt.axis('tight')

F = plt.gcf()
F.set_size_inches(fig_size)
pdf_pages = PdfPages("../plots/images/hyperdex_labelled.pdf")
pdf_pages.savefig(F, bbox_inches='tight')
plt.clf()
pdf_pages.close()