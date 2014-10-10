#
# This plots the document frequencies of all words in the wikipedia dump.
#
plot "df.wiki" using (column(0)):2 title 'Document frequencies' # smooth cumulative
