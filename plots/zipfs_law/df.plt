#
# This plots the document frequencies of all words in the wikipedia dump.
#
set terminal "png"
set output "graph.png"

set xrange [0:60000]
plot "df.wiki" using (column(0)):2 title 'Document frequencies' # smooth cumulative
