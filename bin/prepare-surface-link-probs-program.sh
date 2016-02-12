#!/bin/bash

set -e

RESULTS_FOLDER=${1:-results}

echo "This script is to be run after the wikipedia extraction and before the surface link probs program."
echo "It downloads the surface document counts, concatenates them to two files 12345 and 678910."
echo "These files are then uploaded to surface-document-counts-halfs.wiki and"
echo "can then be used for populating tries."
echo ""
echo "Will work on the folder: ${RESULTS_FOLDER}. Abort in next five seconds if wrong."
sleep 5

echo "Downloading .."
$HADOOP_HOME/bin/hdfs dfs -copyToLocal hdfs://tenemhead2/home/stefan.bunk/$RESULTS_FOLDER/surface-document-counts.wiki

echo "Catting .."
cat surface-document-counts.wiki/{1,2,3,4,5}  > 12345
cat surface-document-counts.wiki/{6,7,8,9,10} > 678910

echo "Statistics:"
ls -lisah 12345 678910
wc -l 12345 678910

echo "Uploading .."
$HADOOP_HOME/bin/hdfs dfs -rmr hdfs://tenemhead2/home/stefan.bunk/$RESULTS_FOLDER/surface-document-counts-halfs.wiki
$HADOOP_HOME/bin/hdfs dfs -mkdir hdfs://tenemhead2/home/stefan.bunk/$RESULTS_FOLDER/surface-document-counts-halfs.wiki
$HADOOP_HOME/bin/hdfs dfs -copyFromLocal 12345  hdfs://tenemhead2/home/stefan.bunk/$RESULTS_FOLDER/surface-document-counts-halfs.wiki/12345
$HADOOP_HOME/bin/hdfs dfs -copyFromLocal 678910 hdfs://tenemhead2/home/stefan.bunk/$RESULTS_FOLDER/surface-document-counts-halfs.wiki/678910

echo "Setting replication"
$HADOOP_HOME/bin/hdfs dfs -setrep 3 hdfs://tenemhead2/home/stefan.bunk/$RESULTS_FOLDER/surface-document-counts-halfs.wiki/

echo "Removing local files"
rm 12345
rm 678910
rm -r surface-document-counts.wiki

