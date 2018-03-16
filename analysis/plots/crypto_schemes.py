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
paillier = []
eegamal = []
ope = []
ore = []
def load_data(filename):
    with open(filename, 'r') as file:
        paillier_encryption, eegamal_encryption, ope_encryption, ore_encryption = [], [], [], []
        paillier_decryption, eegamal_decryption, ope_decryption, ore_decryption = [], [], [], []
        paillier_operation, eegamal_operation, ope_operation, ore_operation = [], [], [], []

        for line in file:
            vals = re.findall('\d+\.\d+', line.rstrip())
            if line.startswith('Enc'):
                paillier_encryption.append(float(vals[0]))
                eegamal_encryption.append(float(vals[1]))
                ope_encryption.append(float(vals[2]))
                ore_encryption.append(float(vals[3]))
            elif line.startswith('Dec'):
                paillier_decryption.append(float(vals[0]))
                eegamal_decryption.append(float(vals[1]))
                ope_decryption.append(float(vals[2]))
                ore_decryption.append(float(vals[3]))
            elif line.startswith('Operation'):
                paillier_operation.append(float(vals[0]))
                eegamal_operation.append(float(vals[1]))
                ope_operation.append(float(vals[2]))
                ore_operation.append(float(vals[3]))
        
        paillier.append(paillier_encryption); paillier.append(paillier_decryption); paillier.append(paillier_operation)
        eegamal.append(eegamal_encryption); eegamal.append(eegamal_decryption); eegamal.append(eegamal_operation)
        ope.append(ope_encryption); ope.append(ope_decryption); ope.append(ope_operation)
        ore.append(ore_encryption); ore.append(ore_decryption); ore.append(ore_operation)

def compute_avg_std(data):
    return np.average(data, axis=1), np.std(data, axis=1)
    
load_data('data/crypto_schemes.data')

#########
#  PLOT #
#########

golden_mean = ((math.sqrt(5) - 1.0) / 2.0) * 0.8
fig_with_pt = 600
inches_per_pt = 1.0 / 72.27 * 2
fig_with = fig_with_pt * inches_per_pt
fig_height = fig_with * golden_mean
fig_size = [fig_with, fig_height]

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
mean_paillier, std_paillier = compute_avg_std(paillier)
mean_eegamal, std_eegamal = compute_avg_std(eegamal)
mean_ope, std_ope = compute_avg_std(ope)
mean_ore, std_ore = compute_avg_std(ore)
#ind = np.arange(len(['Type']))
width = 0.27

fig = plt.figure()
axes = []
for i  in range(3):
    ax = fig.add_subplot(2,3,i+1)
    axes.append(ax)
for i in range(3):
    ax = fig.add_subplot(2,3,i+4, sharex=axes[i])
    axes.append(ax)

if mean_paillier[2] == 0:
    mean_paillier[2] = 0.00000001
if mean_ope[2] == 0:
    mean_ope[2] = 0.00000001

ind = np.arange(len(['test']))
for i in range(3):
    # plot same data in both top and down axes
    rects1 = axes[i].bar(ind, mean_paillier[i], width, color='0.25', yerr=std_paillier[i], error_kw=dict(ecolor='0.90', lw=2, capsize=5, capthick=2))
    rects2 = axes[i].bar(ind+width, mean_eegamal[i], width, color='0.50', yerr=std_eegamal[i], error_kw=dict(ecolor='0.00', lw=2, capsize=5, capthick=2))
    rects3 = axes[i].bar(ind+2*width, mean_ope[i], width, color='0.75', yerr=std_ope[i], error_kw=dict(ecolor='0.25', lw=2, capsize=5, capthick=2))
    rects4 = axes[i].bar(ind+3*width, mean_ore[i], width, color='0.90', yerr=std_ore[i], error_kw=dict(ecolor='0.75', lw=2, capsize=5, capthick=2))

    rects1 = axes[i+3].bar(ind, mean_paillier[i], width, color='0.25', yerr=std_paillier[i], error_kw=dict(ecolor='0.90', lw=2, capsize=5, capthick=2))
    rects2 = axes[i+3].bar(ind+width, mean_eegamal[i], width, color='0.50', yerr=std_eegamal[i], error_kw=dict(ecolor='0.00', lw=2, capsize=5, capthick=2))
    rects3 = axes[i+3].bar(ind+2*width, mean_ope[i], width, color='0.75', yerr=std_ope[i], error_kw=dict(ecolor='0.25', lw=2, capsize=5, capthick=2))
    rects4 = axes[i+3].bar(ind+3*width, mean_ore[i], width, color='0.90', yerr=std_ore[i], error_kw=dict(ecolor='0.75', lw=2, capsize=5, capthick=2))

    axes[i].spines['bottom'].set_visible(False)
    axes[i+3].spines['top'].set_visible(False)
    axes[i].xaxis.tick_top()
    axes[i].tick_params(labeltop='off')  # don't put tick labels at the top
    axes[i+3].xaxis.tick_bottom()

    axes[i].grid(True, linestyle=':', color='0.8', zorder=0)
    axes[i+3].grid(True, linestyle=':', color='0.8', zorder=0)
    axes[i+3].set_xticklabels([])
    
    axes[i].set_xticks(ind + width)
    axes[i+3].set_xticks(ind + width)

    if i == 0:
        axes[i].set_ylim(5.6, 6)
        axes[i+3].set_ylim(0, .4)
    elif i == 1:
        axes[i].set_ylim(.12, .16)
        axes[i+3].set_ylim(0, .02)
    elif i == 2:
        axes[i].set_ylim(.004, .008)
        axes[i+3].set_ylim(0, .0006)

    ## To the cool cut-off
    d = .015  # how big to make the diagonal lines in axes coordinates
    # arguments to pass to plot, just so we don't keep repeating them
    kwargs = dict(transform=axes[i].transAxes, color='k', clip_on=False)
    axes[i].plot((-d, +d), (-d, +d), **kwargs)        # top-left diagonal
    axes[i].plot((1 - d, 1 + d), (-d, +d), **kwargs)  # top-right diagonal

    kwargs.update(transform=axes[i+3].transAxes)  # switch to the bottom axes
    axes[i+3].plot((-d, +d), (1 - d, 1 + d), **kwargs)  # bottom-left diagonal
    axes[i+3].plot((1 - d, 1 + d), (1 - d, 1 + d), **kwargs)  # bottom-right diagonal

    axes[i].autoscale(enable=True, axis='x', tight=True)
    axes[i+3].autoscale(enable=True, axis='x', tight=True)

   
fig.text(0.04, 0.5, 'Time [ms]', va='center', rotation='vertical')
plt.figlegend((rects1[0], rects2[0], rects3[0], rects4[0]), ('Paillier', 'EC ElGamal', 'OPE', 'ORE'), loc="upper center", ncol=4, labelspacing=0.)
axes[3].set_xlabel("Encryption")
axes[4].set_xlabel("Decryption")
axes[5].set_xlabel("Operation")

plt.subplots_adjust(hspace=0.08, wspace=0.4)     

F = plt.gcf()
F.set_size_inches(fig_size)
pdf_pages = PdfPages("../plots/images/crypto_schemes.pdf")
pdf_pages.savefig(F, bbox_inches='tight')
plt.clf()
pdf_pages.close()