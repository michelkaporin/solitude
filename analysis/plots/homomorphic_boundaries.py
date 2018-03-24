import math
from numpy import genfromtxt
from datetime import datetime
import numpy as np
import matplotlib.ticker as ticker

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

x_data = []
paillier_data = []
strawman_data = []
eegamal_data = []

with open("../thesis_prepared_raw_data/timecrypt_boundaries/paillier_performance_requests/paillier_requests.log") as file:
    i = 0
    for line in file:
        if i >= 800000: break
        if line.startswith('Paillier'):
            paillier_data.append(float(line.rstrip().split("\t")[4]))
            i += 1
            x_data.append(i)
            for x in range(98): i += 1; next(file)
with open("../thesis_prepared_raw_data/timecrypt_boundaries/strawman_sum.log") as file:
    i = 0
    for line in file:
        if i >= 100000: break
        if line.startswith('TimeCrypt Baseline SUM'):
            strawman_data.append(float(line.rstrip().split("\t")[4]))
            i += 1
            for x in range(98): i += 1; next(file)
with open("../thesis_prepared_raw_data/timecrypt_boundaries/ecelgamal_ore_cutsum/ecelgamal_ore_cutsum.log") as file:
    i = 0
    for line in file:
        if i >= 800000: break
        if line.startswith('EC ElGamal'):
            eegamal_data.append(float(line.rstrip().split("\t")[4]))
            i += 1
            for x in range(98): i += 1; next(file)

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

colors = ['r', 'g', 'b']
linestyles = ['-', '-', '-']

fig = plt.figure()
ax1 = fig.add_subplot(211)
first, = ax1.plot(x_data, paillier_data, color=colors[0], linestyle=linestyles[0], linewidth=1.5)
second, = ax1.plot(x_data, eegamal_data, color=colors[1], linestyle=linestyles[1], linewidth=1.5)
third, = ax1.plot(x_data[:1011], strawman_data, color=colors[2], linestyle=linestyles[2], linewidth=1.5)
ax1.legend([first, second, third], ['Paillier', 'EC ElGamal','Strawman Sum'], bbox_to_anchor=(-0.02, 1), loc=3, ncol=3, handletextpad=0.3)
ax1.yaxis.set_major_locator(ticker.MaxNLocator(5))

ax2 = fig.add_subplot(212)
ax2.plot(x_data[:250], paillier_data[:250], color=colors[0], linestyle=linestyles[0], linewidth=1.5)
ax2.plot(x_data[:250], eegamal_data[:250], color=colors[1], linestyle=linestyles[1], linewidth=1.5)
ax2.plot(x_data[:250], strawman_data[:250], color=colors[2], linestyle=linestyles[2], linewidth=1.5)

fig.text(0.5, 0.01, 'Chunk intervals', ha='center')
fig.text(0.02, 0.5, 'Time [ms]', va='center', rotation='vertical')

ax1.grid(True, linestyle=':', color='0.8', zorder=0)
ax2.grid(True, linestyle=':', color='0.8', zorder=0)

fig.subplots_adjust(hspace=.5, bottom=0.15)
ax1.legend(loc='best')

F = plt.gcf()
F.set_size_inches(fig_size)
pdf_pages = PdfPages('images/homomorphic_boundaries.pdf')
pdf_pages.savefig(fig, bbox_inches='tight')
plt.savefig('images/homomorphic_boundaries.png', bbox_inches='tight')
plt.clf()
pdf_pages.close()
