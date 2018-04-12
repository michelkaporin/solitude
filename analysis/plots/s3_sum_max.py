import math
from numpy import genfromtxt
from datetime import datetime
import numpy as np
from matplotlib.ticker import MaxNLocator, FuncFormatter
import matplotlib.ticker as ticker

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

sum_data = []
max_data = []

with open("data/s3_sum_max.data") as file:
    sum = False
    for line in file:
        if line.startswith('*** S3 SUM'):
            sum = True
        elif line.startswith('*** S3 MAX'):
            sum = False
        else:
            value = line.rstrip().split("\t")[4]
            if sum:
                sum_data.append(value)
            else:
                max_data.append(value)

########
# PLOT #
########

def format_interval(y, pos=None):
    if y == 0:
        return 1
    else:
        return int(y)
# ---------------------------- GLOBAL VARIABLES --------------------------------#
# figure settings
fig_width_pt = 400.0  # Get this from LaTeX using \showthe
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

pdf_pages = PdfPages('images/s3_sum_max.pdf')

plt.rcParams.update(params)
plt.axes([0.12, 0.32, 0.85, 0.63], frameon=True)
plt.rc('pdf', fonttype=42)  # IMPORTANT to get rid of Type 3

colors = ['0.1', '0.3', '0.6']
linestyles = ['-', '--', '-']

fig, ax = plt.subplots(1, 1)
ax.plot(range(len(sum_data[:1000])), sum_data[:1000],
        color=colors[0], linestyle=linestyles[0], linewidth=2)

ax.xaxis.set_major_formatter(FuncFormatter(format_interval))
ax.xaxis.set_major_locator(ticker.MaxNLocator(10))

#plt.xticks(rotation=75)

plt.ylabel('Time [ms]')
plt.xlabel('Time interval')

plt.grid(True, linestyle=':', color='0.8', zorder=0)
F = plt.gcf()
F.set_size_inches(fig_size)
pdf_pages.savefig(F, bbox_inches='tight')
plt.clf()
pdf_pages.close()
