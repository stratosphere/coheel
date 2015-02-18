#!/bin/sh
cat $1 | grep -oh -P '^.*?(?=\t)' | sort | uniq > $1.unique
wc -l $1.unique
