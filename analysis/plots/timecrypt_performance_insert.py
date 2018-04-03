import math
from numpy import genfromtxt
from datetime import datetime
import numpy as np
import matplotlib.ticker as ticker

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

insert_time = []
chunk_numbers = []

insert_time_precise = []
chunk_numbers_precise = []

with open("../thesis_prepared_raw_data/timecrypt_boundaries/paillier_performance_requests/paillier_performance.log") as file:
    i = 0;
    for line in file:
        line = line.rstrip().split("\t")

        if line[3] != 'insert': continue

        chunk_count = int(line[5])
        time = float(line[4])

        if chunk_count == 1:
            insert_time.append(time)
            chunk_numbers.append(chunk_count)
            continue
        if chunk_count % 100 == 0:
            insert_time.append(time)
            chunk_numbers.append(chunk_count)
        
        if chunk_count <= 1600:
            insert_time_precise.append(time)
            chunk_numbers_precise.append(chunk_count)

########
# PLOT #
########

# ---------------------------- GLOBAL VARIABLES --------------------------------#
# figure settings
fig_width_pt = 300.0  # Get this from LaTeX using \showthe
inches_per_pt = 1.0 / 72.27 * 2  # Convert pt to inches
golden_mean = ((math.sqrt(5) - 1.0) / 2.0) * .8  # Aesthetic ratio
fig_width = fig_width_pt * inches_per_pt  # width in inches
fig_height = (fig_width * golden_mean)  # height in inches
fig_size = [fig_width, fig_height]

params = {'backend': 'ps',
          'axes.labelsize': 20,
          'legend.fontsize': 18,
          'xtick.labelsize': 18,
          'ytick.labelsize': 18,
          'font.size': 18,
          'figure.figsize': fig_size,
          'font.family': 'times new roman'}

plt.rcParams.update(params)
plt.rc('pdf', fonttype=42)  # IMPORTANT to get rid of Type 3

color = '0.1'
linestyles = ['-', '-', '-']

fig = plt.figure()
ax1 = fig.add_subplot(211)
second, = ax1.plot(chunk_numbers, insert_time, color=color, linestyle=linestyles[1], linewidth=1.5)
ax1.yaxis.set_major_locator(ticker.MaxNLocator(5))
ax1.set_ylim([0,10])

ax2 = fig.add_subplot(212)
ax2.plot(chunk_numbers_precise, insert_time_precise, color=color, linestyle=linestyles[1], linewidth=1.5)
ax2.yaxis.set_major_locator(ticker.MaxNLocator(5))

fig.text(0.5, 0.01, 'Chunk count', ha='center')
fig.text(0.02, 0.5, 'Insert time [ms]', va='center', rotation='vertical')

ax1.grid(True, linestyle=':', color='0.8', zorder=0)
ax2.grid(True, linestyle=':', color='0.8', zorder=0)

fig.subplots_adjust(hspace=.5, bottom=0.15)
ax1.legend(loc='best')

F = plt.gcf()
F.set_size_inches(fig_size)
pdf_pages = PdfPages('images/timecrypt_performance_insert.pdf')
pdf_pages.savefig(fig, bbox_inches='tight')
plt.clf()
pdf_pages.close()
