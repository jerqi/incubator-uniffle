#!/usr/bin/env bash

set -o pipefail
set -e

COORDINATOR_HOME="$(
  cd "$(dirname "$0")/.."
  pwd
)"
CONF_FILE="./conf/coordinator.conf "
MAIN_CLASS="com.tencent.rss.coordinator.CoordinatorServer"

source "${COORDINATOR_HOME}/bin/rss-env.sh"

cd $COORDINATOR_HOME

JAR_DIR="./jars"

for file in $(ls ${JAR_DIR}/coordinator/*.jar 2>/dev/null); do
  CLASSPATH=$CLASSPATH:$file
done

echo "class path is $CLASSPATH"

JVM_ARGS=" -server \
          -Xmx8g \
          -XX:+UseG1GC \
          -XX:MaxGCPauseMillis=200 \
          -XX:ParallelGCThreads=20 \
          -XX:ConcGCThreads=5 \
          -XX:InitiatingHeapOccupancyPercent=45 "

if [ -f ./conf/log4j.properties ]; then
  ARGS="$ARGS -Dlog4j.configuration=file:./conf/log4j.properties"
else
  echo "Exit with error: $conf/log4j.properties file doesn't exist."
  exit 1
fi

$RUNNER $ARGS $JVM_ARGS $JAVA_LIB_PATH -cp $CLASSPATH $MAIN_CLASS --conf $CONF_FILE $@ &

echo $! >$COORDINATOR_HOME/currentpid
