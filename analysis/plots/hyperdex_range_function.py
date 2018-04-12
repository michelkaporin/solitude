import math
from numpy import genfromtxt
from datetime import datetime
import numpy as np
from matplotlib.ticker import MaxNLocator, FuncFormatter

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

y_data = []
x_data = []


def aggregate():
    vals = []
    for data in y_data:
        arr = np.asarray([row[1] for row in data])
        arr = arr.astype(float)
        vals.append(arr)
    vals = np.asarray(vals)
    return vals.mean(axis=0)


with open("../thesis_prepared_raw_data/hyperdex_benchmark_4_1000_3.log") as file:
    for _ in xrange(5):
        next(file)  # Skip header lines until the first data

    reps = 1
    arr = []
    for line in file:
        if line.startswith(('[1]', '.::')):
            y_data.append(arr)
            arr = []  # new repetition
            reps += 1
            continue
        if reps > 3:
            break

        arr.append(line.rstrip().split("\t"))

x_data = [row[0] for row in y_data[0]]
y_data = aggregate()

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

pdf_pages = PdfPages('images/hyperdex_range_function.pdf')

plt.rcParams.update(params)
plt.axes([0.12, 0.32, 0.85, 0.63], frameon=True)
plt.rc('pdf', fonttype=42)  # IMPORTANT to get rid of Type 3

colors = ['0.1', '0.3', '0.6']
linestyles = ['-', '--', '-']

fig, ax = plt.subplots(1, 1)
ax.plot(range(1, len(x_data)+1), y_data,
        color=colors[0], linestyle=linestyles[0], linewidth=1.5)

ax.xaxis.set_major_formatter(FuncFormatter(format_interval))
ax.xaxis.set_major_locator(MaxNLocator(nbins=10, prune=None))
#plt.xticks(rotation=75)

plt.ylabel('Time [ms]')
plt.xlabel('Interval of single entries')

plt.grid(True, linestyle=':', color='0.8', zorder=0)
F = plt.gcf()
F.set_size_inches(fig_size)
pdf_pages.savefig(F, bbox_inches='tight')
plt.clf()
pdf_pages.close()
