#!/usr/bin/env bash

set -o pipefail
set -e

SHUFFLE_SERVER_HOME="$(
  cd "$(dirname "$0")/.."
  pwd
)"

cd $SHUFFLE_SERVER_HOME

bash ./bin/stop-shuffle-server.sh
sleep 3
bash ./bin/start-shuffle-server.sh

exit 0