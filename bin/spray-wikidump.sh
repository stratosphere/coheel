#! /bin/bash


default_val="http://dumps.wikimedia.org/enwiki/20151102/enwiki-20151102-pages-articles.xml.bz2"
read -p "Enter dump file url (default: $default_val): " tmp
DUMPURL=${tmp:-$default_val}
DUMPFILE="$(echo $DUMPURL | rev | cut -d/ -f1 | cut -d. -f2- | rev)"
DUMPDATE="$(echo $DUMPURL | rev | cut -d/ -f2 | rev)"
default_val="hdfs:///data/$DUMPDATE/raw/"
read -p "Enter HDFS raw data folder (default: $default_val): " tmp
HDFSRAW=${tmp:-$default_val}

hadoop fs -mkdir -p $HDFSRAW
echo downloading $DUMPURL and spraying wikidump to $HDFSRAW$DUMPFILE
curl -# $DUMPURL | bzip2 -dc | hadoop fs -put - $HDFSRAW$DUMPFILE
# local file bzcat .../$DUMPFILE.bz2 | pv | hadoop fs -put - $HDFSRAW$DUMPFILE
