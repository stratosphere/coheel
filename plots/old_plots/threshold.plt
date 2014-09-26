#
# Evaluation for the threshold cut-off.
# This is obsolete now, because it usede the join-approach for determining the entire text surface counts.
#

set datafile separator "\t"
set xlabel "Threshold in % of # number of pages\nEverything above threshold is thrown away."
set ylabel "# of potentially missed mentions"
set title "Plotting threshold against # of potentially missed mentions"
plot "threshold.wiki" using 1:2
