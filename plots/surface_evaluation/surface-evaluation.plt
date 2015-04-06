#
# Precision-Recall graph for evaluating the best surface threshold
#

set terminal wxt size 800,700
set key left bottom
set xlabel "Threshold"
set ylabel "Performance"
set datafile separator "\t"
plot 'surface-evaluation-big.tsv' using 1:2 with linespoints ps 2 pt  7 title 'Precision', \
     'surface-evaluation-big.tsv' using 1:3 with linespoints ps 2 pt  9 title 'Precision with Subsets', \
     'surface-evaluation-big.tsv' using 1:4 with linespoints ps 2 pt 11 title 'Recall', \
     'surface-evaluation-big.tsv' using 1:5 with linespoints ps 2 pt 13 title 'F1'

