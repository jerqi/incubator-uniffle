#!/usr/bin/env bash

set -o pipefail
set -e

SHUFFLE_SERVER_HOME="$(
  cd "$(dirname "$0")/.."
  pwd
)"

cd $SHUFFLE_SERVER_HOME

source "${SHUFFLE_SERVER_HOME}/bin/rss-env.sh"

HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
HADOOP_DEPENDENCY=$HADOOP_HOME/etc/hadoop:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/yarn/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_HOME/share/hadoop/mapreduce/*

CONF_FILE="./conf/server.conf "
MAIN_CLASS="com.tencent.rss.server.ShuffleServer"

JAR_DIR="./jars"

for file in $(ls ${JAR_DIR}/server/*.jar 2>/dev/null); do
  CLASSPATH=$CLASSPATH:$file
done

if [ -z "$HADOOP_HOME" ]; then
  echo "No env HADOOP_HOME."
  exit 1
fi

if [ -z "$HADOOP_CONF_DIR" ]; then
  echo "No env HADOOP_CONF_DIR."
  exit 1
fi

if [ -z "$XMX_SIZE" ]; then
  echo "No jvm xmx size"
  exit 1
fi

echo "Using Hadoop from $HADOOP_HOME"

CLASSPATH=$CLASSPATH:$HADOOP_CONF_DIR:$HADOOP_DEPENDENCY
JAVA_LIB_PATH="-Djava.library.path=$HADOOP_HOME/lib/native"

JVM_ARGS=" -server \
          -Xmx${XMX_SIZE} \
          -Xms${XMX_SIZE} \
          -XX:+UseG1GC \
          -XX:MaxGCPauseMillis=200 \
          -XX:ParallelGCThreads=20 \
          -XX:ConcGCThreads=5 \
          -XX:InitiatingHeapOccupancyPercent=20 \
          -XX:G1HeapRegionSize=32m \
          -XX:+UnlockExperimentalVMOptions \
          -XX:G1NewSizePercent=10 \
          -XX:+PrintGC \
          -XX:+PrintAdaptiveSizePolicy \
          -XX:+PrintGCDateStamps \
          -XX:+PrintGCTimeStamps \
          -XX:+PrintGCDetails \
          -Xloggc:./logs/gc.log"

if [ -f ./conf/log4j.properties ]; then
  ARGS="$ARGS -Dlog4j.configuration=file:./conf/log4j.properties"
else
  echo "Exit with error: $conf/log4j.properties file doesn't exist."
  exit 1
fi

$RUNNER $ARGS $JVM_ARGS $JAVA_LIB_PATH -cp $CLASSPATH $MAIN_CLASS --conf $CONF_FILE $@ &

echo $! >$SHUFFLE_SERVER_HOME/currentpid
