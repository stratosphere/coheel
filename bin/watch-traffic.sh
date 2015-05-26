#!/usr/bin/env /bin/bash

DEVICE="wlan0"
PERIOD=3000
echo "Listening to traffic on $DEVICE. Press ^C to abort."
nload -t $PERIOD -u B $DEVICE > $DEVICE-traffic
grep -o -P "Incoming.*?Curr.*?/s" $DEVICE-traffic | grep -P --only-matching " \d+" > foo

LINES=`wc -l < foo`
echo "Recorded $LINES signals."
LINES=$((LINES * PERIOD))
seq 0 $PERIOD $LINES | paste -d '' - foo | tee output | feedgnuplot --domain --lines --points --legend 0 "data 0" --title "Incoming network traffic"
rm foo
