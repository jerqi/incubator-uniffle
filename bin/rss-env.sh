#!/usr/bin/env bash

set -o pipefail
set -e

JAVA_HOME=<java_home_dir>
HADOOP_HOME=<hadoop_home_dir>
XMX_SIZE="64g"

RUNNER="${JAVA_HOME}/bin/java"
