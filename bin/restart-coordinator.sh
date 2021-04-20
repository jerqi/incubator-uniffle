#!/usr/bin/env bash

set -o pipefail
set -e

COORDINATOR_HOME="$(
  cd "$(dirname "$0")/.."
  pwd
)"

cd $COORDINATOR_HOME

bash ./bin/stop-coordinator.sh
sleep 3
bash ./bin/start-coordinator.sh

exit 0