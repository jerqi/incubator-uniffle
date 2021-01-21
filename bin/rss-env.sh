#!/usr/bin/env bash

set -o pipefail
set -e

if [ -z "$JAVA_HOME" ]; then
  if [ "$(command -v java)" ]; then
    RUNNER=$(command -v java)
  else
    echo "No JAVA_HOME"
    exit 1
  fi
else
  RUNNER="${JAVA_HOME}/bin/java"
fi

HADOOP_HOME="/data/software/hadoop-2.7.5"
DEFAULT_HOME_HOME="/data/gaiaadmin/gaiaenv/tdwgaia/"

if [ -z "$HADOOP_HOME" ]; then
  if [ -d $DEFAULT_HOME_HOME ]; then
    HADOOP_HOME=$DEFAULT_HOME_HOME
    echo "HADOOP_HOME is not set, use gaia default $HADOOP_HOME"
  else
    echo "HADOOP_HOME is not set, and no gaia default, hdfs operation will not run properly."
    exit 1
  fi
fi

export HADOOP_HOME=$HADOOP_HOME
export HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/yarn/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_HOME/share/hadoop/mapreduce/*
