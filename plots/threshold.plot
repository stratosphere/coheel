set datafile separator "\t"
set xlabel "Threshold in % of # number of pages\nEverything above threshold is thrown away."
set ylabel "# of potentially missed mentions"
set title "Plotting threshold against # of potentially missed mentions"
plot "threshold-evaluation.wiki" using 1:2
