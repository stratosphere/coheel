#
# ROC curve for setting the NER threshold
#

set terminal wxt size 800,700
set xrange [0:1]
set yrange [0:1]
set xlabel "FPR"
set ylabel "TPR"
set key right bottom
set datafile separator "\t"
plot 'ner-roc-curve.tsv' using 7:6 title 'ROC curve' pt 7, \
	 x, \
     'ner-roc-curve.tsv' using 7:6:1 with labels offset 3, 0, 5, 0 notitle \
