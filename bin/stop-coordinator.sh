#!/usr/bin/env bash

set -o pipefail
set -o nounset   # exit the script if you try to use an uninitialised variable
set -o errexit   # exit the script if any statement returns a non-true return value
set -e

SCRIPT_DIR="$(cd "`dirname "$0"`"; pwd)"
BASE_DIR="$(cd "`dirname "$0"`/.."; pwd)"

source "$SCRIPT_DIR/utils.sh"
common_shutdown "coordinator" ${BASE_DIR}