#!/bin/bash

set -e

for var in "$@"; do
	echo "Delete up to first occurrence of <page>"
	sed -n '/<page>/,$p' $var > $var.tmp1
	echo "Add markers in between"
	awk '/<\/page>/{print;print "<MARKER />";next}1' $var.tmp1 > $var.tmp2
	echo "Remove last two lines"
	head -n -2 $var.tmp2 > $var.tmp3
	rm $var.tmp1 $var.tmp2
	mv $var.tmp3 $var
done
