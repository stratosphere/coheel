mkdir stefan-wikidata
cd stefan-wikidata
scp "isfet:/data/wikipedia/*.dump" .
hadoop dfs -copyFromLocal *.dump hdfs://tenemhead2/user/stefan.bunk/
