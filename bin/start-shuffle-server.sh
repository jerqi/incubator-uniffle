#!/usr/bin/env bash

set -o pipefail
set -e

SHUFFLE_SERVER_HOME="$(cd "`dirname "$0"`/.."; pwd)"
CONF_FILE="./conf/server.conf "
MAIN_CLASS="com.tencent.rss.server.ShuffleServer"

source "${SHUFFLE_SERVER_HOME}/bin/rss-env.sh"

cd $SHUFFLE_SERVER_HOME

JAR_DIR="./jars"

for file in $(ls ${JAR_DIR}/*.jar 2>/dev/null);
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $(ls ${JAR_DIR}/server/*.jar 2>/dev/null);
do
  CLASSPATH=$CLASSPATH:$file
done

echo "class path is $CLASSPATH"

if [ -z "$HADOOP_HOME" ]; then
  echo "No env HADOOP_CONF_DIR."
  exit 1
fi

if [ -z "$HADOOP_CONF_DIR" ]; then
  echo "No env HADOOP_CONF_DIR."
  exit 1
fi

if [ -z "$YARN_CONF_DIR" ]; then
  echo "No env YARN_CONF_DIR."
  exit 1
fi

echo "Using Hadoop from $HADOOP_HOME"

CLASSPATH=$CLASSPATH:$HADOOP_CONF_DIR/*:$HADOOP_HOME/*:$YARN_CONF_DIR/*
JAVA_LIB_PATH="-Djava.library.path=$HADOOP_HOME/lib/native"

JVM_ARGS=" -server \
          -Xmx64g \
          -Xms64g \
          -XX:PermSize=512m \
          -XX:+UseG1GC \
          -XX:MaxGCPauseMillis=200 \
          -XX:ParallelGCThreads=20 \
          -XX:ConcGCThreads=5 \
          -XX:InitiatingHeapOccupancyPercent=70 "

if [ -f ./conf/log4j.properties ]; then
  ARGS="$ARGS -Dlog4j.configuration=file:./conf/log4j.properties"
else
  echo "Exit with error: $conf/log4j.properties file doesn't exist."
  exit 1;
fi

$RUNNER $ARGS $JVM_ARGS $JAVA_LIB_PATH -cp $CLASSPATH $MAIN_CLASS --conf $CONF_FILE $@ &

echo $! > $SHUFFLE_SERVER_HOME/currentpid
