
plot:
	gnuplot -p *.plt

copy:
	cd ../.. && bin/download-from-hdfs results surface-evaluation && cd -
	cp ../../cluster-output/surface-evaluation.wiki ./surface-evaluation.tsv
