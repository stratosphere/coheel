set terminal "png"
set output "graph.png"

# Parameters
n     = 100 # number of intervals
max   = 1500.
min   = 0.
width = (max - min) / n

hist(x, width) = width * floor(x / width) + width / 2.0

set xrange [min:max]
set yrange [0:]

#to put an empty boundary around the
#data inside an autoscaled graph.
set offset graph 0.05,0.05,0.05,0.0

set xtics min, (max - min) / 5, max
set boxwidth width*0.9
set style fill solid 0.5 #fillstyle
set tics out nomirror
set xlabel "Unique words in article"
set ylabel "Frequency"

plot "data" using (hist($1, width)):(1.0) smooth freq with boxes lc rgb"red" notitle
