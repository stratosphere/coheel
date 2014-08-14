set terminal wxt size 800,700
set xrange [0:1]
set yrange [0:1]
set key right bottom
set datafile separator ","
plot 'roc.csv' using 1:2 title 'ROC curve' pt 7, \
	 x, \
     'roc.csv' using 1:2:3 with labels offset 3, 0, 5, 0 notitle, \
