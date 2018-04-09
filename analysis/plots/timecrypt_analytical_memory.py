import math
from numpy import genfromtxt
from datetime import datetime
import numpy as np
import re
import matplotlib.ticker as ticker

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

chunks_count = []
paillier = []
ecelgamal = []
ope = []
ore = []
plaintext = []

with open("data/analytical_memory.data") as file:
    state = 0 # 1: Chunks Count; 2: Paillier; 3: EC ElGamal; 4: OPE; 5: ORE; 6: Plaintext

    for line in file:
        if line in ['\n', '\r\n']:
            continue
        elif line.startswith('Chunks Count'):
            state = 1
        elif line.startswith('Paillier'):
            state = 2
        elif line.startswith('EC ElGamal'):
            state = 3
        elif line.startswith('OPE'):
            state = 4
        elif line.startswith('ORE'):
            state = 5
        elif line.startswith('Plaintext'):
            state = 6
        else:
            value = float(line.rstrip())
            if state == 1:
                chunks_count.append(int(line.rstrip()))
            elif state == 2 :
                paillier.append(value)
            elif state == 3 :
                ecelgamal.append(value)
            elif state == 4 :
                ope.append(value)
            elif state == 5 :
                ore.append(value)
            elif state == 6 :
                plaintext.append(value)

########
# PLOT #
########

def format_size(y, pos=None):
    print y
    return 2**y
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

colors = ['r', 'g', 'b', '']


fig, ax = plt.subplots(1, 1)
first, = ax.plot(chunks_count, plaintext, linewidth=1.5)
second, = ax.plot(chunks_count, paillier, linewidth=1.5)
third, = ax.plot(chunks_count, ecelgamal, linewidth=1.5)
fourth, = ax.plot(chunks_count, ope, linewidth=1.5)
fifth, = ax.plot(chunks_count, ore, linewidth=1.5)

ax.legend([first, second, third, fourth, fifth], ['Plaintext', 'Paillier', 'EC-ElGamal', 'OPE', 'ORE'], bbox_to_anchor=(0, 1), loc=3, ncol=5, handletextpad=0.3)
plt.yscale('log', basey=2)
plt.xscale('log', basex=2)

# ax.get_xaxis().get_major_formatter().labelOnlyBase = False
# ax1.set_xticks([20, 200, 500])
# ax.get_xaxis().set_major_formatter(ticker.ScalarFormatter())
# ax.yaxis.set_major_formatter(ticker.FuncFormatter(format_size))
# ax.xaxis.set_major_formatter(ticker.ScalarFormatter(useOffset=False))
# ax.get_xaxis().get_major_formatter().set_scientific(False)

plt.xticks(chunks_count[0::2])

plt.xticks(rotation=30)
plt.ylabel('Memory [MB]')
plt.xlabel('Chunk count')

pdf_pages = PdfPages('images/timecrypt_analytical_memory.pdf')
plt.grid(True, linestyle=':', color='0.8', zorder=0)
F = plt.gcf()
F.set_size_inches(fig_size)
pdf_pages.savefig(F, bbox_inches='tight')
plt.clf()
pdf_pages.close()