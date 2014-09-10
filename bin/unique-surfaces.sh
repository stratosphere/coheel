#!/bin/sh
cat output/surface-probs.wiki | grep -oh -P '^.*?(?=\t)' | sort | uniq > output/surfaces.wiki
wc -l output/surfaces.wiki
