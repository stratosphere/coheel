#! /bin/bash

FLINK_BINARY=http://www.eu.apache.org/dist/flink/flink-0.10.1/flink-0.10.1-bin-hadoop26-scala_2.10.tgz
FLINK_FOLDER=flink-0.10.1

# install only on master 
if [ "$(grep isMaster /mnt/var/lib/info/instance.json | cut -d: -f2 | tr -d ' ')" == 'true' ]; then
  # get binaries
  mkdir -p ~/flink
  wget $FLINK_BINARY -O ~/flink/flink.tgz
  tar xvzf ~/flink/flink.tgz -C ~/flink
  # setup
  echo -e "\nexport YARN_CONF_DIR=/etc/hadoop/conf" >> ~/flink/$FLINK_FOLDER/bin/config.sh
  if [ ! -e ~/flink/default ]; then ln -s ~/flink/$FLINK_FOLDER ~/flink/default; fi
  echo -e "\nexport PATH=\$PATH:~/flink/default/bin" >> ~/.bashrc
  # cleanup
  rm ~/flink/flink.tgz
fi
