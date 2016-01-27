## coheel
A library for the automatic detection and disambiguation of knowledge base entity mentions in texts.

### Setup

Programs can be run via the `bin/run` script.
All programs need a `--configuration` parameter, which identifies a file under `src/main/resources`.
This file configures required properties, such as job manager, hdfs, path to certain files etc.

### Run preprocessing and classification scripts

    # preprocessing: extract main data like surfaces, links, redirects, language models, etc.
    bin/run --configuration cluster_tenem --program extract-main

    # extract probability that a surface is linked at all
    bin/prepare-surface-link-probs-program.sh
    bin/run --configuration cluster_tenem --program surface-link-probs

    # create training data
    bin/prepare-tries.sh
	# .. upload tries manually to locations specified in the configuration
    bin/run --configuration cluster_tenem --program training-program
    # training
    mvn scala:run -Dlauncher=MachineLearningTestSuite

    # classification
    bin/run --configuration cluster_tenem --program classification --parallelism 10
