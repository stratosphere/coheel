#!/bin/sh

echo "Processing $1"

echo "Removing everything before first occurrence"
# Remove everything before first occurrence of </siteinfo> - everything before the pages start
sed -i '0,/<\/siteinfo>/d' $1

echo "Adding markers"
# Add markers after each end page
sed -i '/<\/page>/a <MARKER />' $1

mv $1 $1.tmp
echo "Removing last two lines"
head -n -2 $1.tmp > $1

rm $1.tmp

