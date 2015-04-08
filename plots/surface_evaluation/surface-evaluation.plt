#
# Precision-Recall graph for evaluating the best surface threshold
#

set terminal wxt size 800,700
set key left bottom
set xlabel "Threshold"
set ylabel "Performance"
set datafile separator "\t"

set style data linespoints

file = 'surface-evaluation-big.tsv'
plot file using 1:2 ps 2 pt  7 title 'Precision', \
     file using 1:4 ps 2 pt 11 title 'Recall', \
     file using 1:5 ps 2 pt 13 title 'F1'
#     file using 1:3 ps 2 pt  9 title 'Precision with Subsets', \

set key right bottom

file = 'surface-evaluation.tsv'
#plot file using 1:3 ps 2 pt  7 title 'Precision', \
#     file using 1:2 ps 2 pt  9 title 'Filter ratio', \
#     file using 1:5 ps 2 pt 11 title 'Recall', \
#     file using 1:6 ps 2 pt 13 title 'F1'
#     file using 1:4 ps 2 pt  9 title 'Precision with Subsets', \

#1.000	1.0	1.0	1.0	0.0553970298012834	0.10497855922849034

