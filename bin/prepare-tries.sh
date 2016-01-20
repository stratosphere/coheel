#!/bin/bash

set -e

echo "This script is to be run after the surface link program and before the training data program."
echo "It downloads the surface links probs, concatenates them to two files 12345 and 678910."
echo "These files can then be uploaded to the tenems."
echo "We do this by uploading to tenemhead and then from there to the tenems, because its faster."

echo "Downloading .."
$HADOOP_HOME/bin/hdfs dfs -copyToLocal hdfs://tenemhead2/home/stefan.bunk/results/surface-link-probs.wiki

cat surface-link-probs.wiki/12345/* > 12345
cat surface-link-probs.wiki/678910/* > 678910

scp  12345 tenemhead2:~
scp 678910 tenemhead2:~
