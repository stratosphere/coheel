#! /usr/bin/python

import json
import os
import urllib
import tarfile

FLINK_BINARY = 'http://www.eu.apache.org/dist/flink/flink-0.10.1/flink-0.10.1-bin-hadoop26-scala_2.10.tgz'
basedir = os.environ['HOME']+'/'


with open('/mnt/var/lib/info/instance.json') as conf_file:                                                                                   
  conf = json.load(conf_file)

if ( conf['isMaster']  ) :
  os.makedirs(basedir)
  # download and extract binary
  tmpfile = basedir+'tmp.tgz'
  urllib.urlretrieve(FLINK_BINARY, tmpfile)
  archive = tarfile.open(tmpfile,'r:gz')
  archive.extractall(basedir)
  flinkdir = basedir+os.path.commonprefix(archive.getnames())
  os.remove(tmpfile)
  # add yarn conf to config
  with open(flinkdir+'/bin/config.sh', "a") as config:
    config.write("\nexport YARN_CONF_DIR=/etc/hadoop/conf")
  default = basedir+'flink'
  if ( not os.path.exists(default) ) :
    os.symlink(flinkdir, basedir+'flink')
    with open(os.environ['HOME']+'/.bashrc', "a") as bashrc:
      bashrc.write("\nexport PATH=$PATH:"+default+"/bin")

