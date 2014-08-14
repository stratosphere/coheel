#!/bin/sh
cat testoutput/surface-probs.wiki | grep -oh -P '^.*?(?=\t)' | sort | uniq > testoutput/surfaces.wiki
wc -l testoutput/surfaces.wiki
