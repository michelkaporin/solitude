import math
from numpy import genfromtxt
from datetime import datetime
import numpy as np
import re
import matplotlib.ticker as ticker
from matplotlib.ticker import MaxNLocator, FuncFormatter

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

baseline_sum_data = []
baseline_max_data = []

timecrypt_paillier_data = []
timecrypt_ecelgamal_data = []

timecrypt_ope_data = []
timecrypt_ore_data = []

with open("../raw_data/TimeCrypt Baseline vs TimeCrypt Varied K.log") as file:
    for _ in xrange(16): next(file)  # Skip header lines until the first data

    state = 0 # 1: Baseline Sum; 2: Baseline Max; 3: TimeCrypt Paillier; 4: TimeCrypt ECElGamal; 5: TimeCrypt OPE; 6: Timecrypt ORE
    experiment = 0
    k = 0

    c_baseline_sum_data = []
    c_baseline_max_data = []

    c_timecrypt_paillier_data = []
    c_timecrypt_ecelgamal_data = []

    c_timecrypt_ope_data = []
    c_timecrypt_ore_data = []

    for line in file:
        if line in ['\n', '\r\n']:
            continue
        elif line.startswith('Experiment number #'):
            experiment = experiment + 1

            if experiment > 1:
                baseline_sum_data.append(c_baseline_sum_data)
                baseline_max_data.append(c_baseline_max_data)

                timecrypt_paillier_data.append(c_timecrypt_paillier_data)
                timecrypt_ecelgamal_data.append(c_timecrypt_ecelgamal_data)

                timecrypt_ope_data.append(c_timecrypt_ope_data)
                timecrypt_ore_data.append(c_timecrypt_ore_data)
        elif line.startswith('*** Baseline SUM ***'):
            state = 1
        elif line.startswith('*** Baseline MAX ***'):
            state = 2
        elif line.startswith('K ='):
            ks = re.findall(r'\d+', line) #extract number
            k = int(ks[0])
        elif line.startswith('*** TimeCrypt Paillier SUM ***'):
            state = 3
        elif line.startswith('*** TimeCrypt EC El Gamal SUM ***'):
            state = 4
        elif line.startswith('*** TimeCrypt Order-Preserving Encryption MAX ***'):
            state = 5
        elif line.startswith('*** TimeCrypt Order-Revealing Encryption MAX ***'):
            state = 6
        elif k == 0 or k == 2:
            value = float(line.rstrip().split("\t")[4])
            if state == 1:
                c_baseline_sum_data.append(value)
            elif state == 2 :
                c_baseline_max_data.append(value)
            elif state == 3 :
                c_timecrypt_paillier_data.append(value)
            elif state == 4 :
                c_timecrypt_ecelgamal_data.append(value)
            elif state == 5 :
                c_timecrypt_ope_data.append(value)
            elif state == 6 :
                c_timecrypt_ore_data.append(value)

def aggregate(input):
    return np.asarray(input).mean(axis=0)

########
# PLOT #
########
def format_interval(y, pos=None):
    if y == 0:
        return 1
    else:
        return int(y)

baseline_max_data = aggregate(baseline_max_data)
baseline_sum_data = aggregate(baseline_sum_data)
timecrypt_paillier_data = aggregate(timecrypt_paillier_data)
timecrypt_ecelgamal_data = aggregate(timecrypt_ecelgamal_data)
timecrypt_ope_data = aggregate(timecrypt_ope_data)
timecrypt_ore_data = aggregate(timecrypt_ore_data)
x_data = range(len(baseline_max_data[:1000]))

# ---------------------------- GLOBAL VARIABLES --------------------------------#
# figure settings
fig_width_pt = 500.0  # Get this from LaTeX using \showthe
inches_per_pt = 1.0 / 72.27 * 2  # Convert pt to inches
golden_mean = ((math.sqrt(5) - 1.0) / 2.0) * .8  # Aesthetic ratio
fig_width = fig_width_pt * inches_per_pt  # width in inches
fig_height = (fig_width * golden_mean)  # height in inches
fig_size = [fig_width, fig_height / 1.22]

params = {'backend': 'ps',
          'axes.labelsize': 20,
          'legend.fontsize': 18,
          'xtick.labelsize': 18,
          'ytick.labelsize': 18,
          'font.size': 18,
          'figure.figsize': fig_size,
          'font.family': 'times new roman'}

plt.rcParams.update(params)
plt.axes([0.12, 0.32, 0.85, 0.63], frameon=True)
plt.rc('pdf', fonttype=42)  # IMPORTANT to get rid of Type 3

colors = ['0', '0.33', '0.66']


### SUM PLOT ###
fig, ax = plt.subplots(1, 1)
first, = ax.plot(x_data, baseline_sum_data[:1000], color=colors[2], linewidth=2)
second, = ax.plot(x_data, timecrypt_paillier_data[:1000], color=colors[1], linewidth=2)
third, = ax.plot(x_data, timecrypt_ecelgamal_data[:1000], color=colors[0], linewidth=2)
ax.legend([first, second, third], ['Strawman Sum', 'Paillier','EC-ElGamal'], bbox_to_anchor=(0, 1), loc=3, ncol=3, handletextpad=0.3)
ax.yaxis.set_major_locator(ticker.MaxNLocator(10))
ax.xaxis.set_major_formatter(FuncFormatter(format_interval))
plt.ylabel('Time [ms]')
plt.xlabel('Time interval')
ax.set_ylim([0,80])

pdf_pages = PdfPages('images/timecrypt_fixed_k_sum.pdf')
plt.grid(True, linestyle=':', color='0.8', zorder=0)
F = plt.gcf()
F.set_size_inches(fig_size)
pdf_pages.savefig(F, bbox_inches='tight')
plt.clf()
pdf_pages.close()


### MAX PLOT ###
fig, ax = plt.subplots(1, 1)
ax.plot(x_data, baseline_max_data[:1000], color=colors[2], linewidth=2)
ax.plot(x_data, timecrypt_ope_data[:1000], color=colors[1], linewidth=2)
ax.plot(x_data, timecrypt_ore_data[:1000], color=colors[0], linewidth=2)
ax.legend([first, second, third], ['Strawman Max', 'OPE','ORE'], bbox_to_anchor=(0, 1), loc=3, ncol=3, handletextpad=0.3)
ax.yaxis.set_major_locator(ticker.MaxNLocator(10))
ax.xaxis.set_major_formatter(FuncFormatter(format_interval))
plt.ylabel('Time [ms]')
plt.xlabel('Time interval')
ax.set_ylim([0,38])

pdf_pages = PdfPages('images/timecrypt_fixed_k_max.pdf')
plt.grid(True, linestyle=':', color='0.8', zorder=0)
F = plt.gcf()
F.set_size_inches(fig_size)
pdf_pages.savefig(F, bbox_inches='tight')
plt.clf()
pdf_pages.close()
