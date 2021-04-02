#! /usr/bin/env bash

set -o pipefail
set -e
set -x
set -u

NAME="rss"
MVN="mvn"
RSS_HOME="$(
  cd "$(dirname "$0")"
  pwd
)"

function exit_with_usage() {
  set +x
  echo "$0 - tool for making binary distributions of Rmote Shuffle Service"
  echo ""
  echo "usage:"
  exit 1
}

cd $RSS_HOME

if [ -z "$JAVA_HOME" ]; then
  echo "Error: JAVA_HOME is not set, cannot proceed."
  exit -1
fi

if [ $(command -v git) ]; then
  GITREV=$(git rev-parse --short HEAD 2>/dev/null || :)
  if [ ! -z "$GITREV" ]; then
    GITREVSTRING=" (git revision $GITREV)"
  fi
  unset GITREV
fi

VERSION=$("$MVN" help:evaluate -Dexpression=project.version $@ 2>/dev/null |
  grep -v "INFO" |
  grep -v "WARNING" |
  tail -n 1)

echo "RSS version is $VERSION"

export MAVEN_OPTS="${MAVEN_OPTS:--Xmx2g -XX:ReservedCodeCacheSize=1g}"

# Store the command as an array because $MVN variable might have spaces in it.
# Normal quoting tricks don't work.
# See: http://mywiki.wooledge.org/BashFAQ/050
BUILD_COMMAND=("$MVN" clean package -DskipTests $@)

# Actually build the jar
echo -e "\nBuilding with..."
echo -e "\$ ${BUILD_COMMAND[@]}\n"

"${BUILD_COMMAND[@]}"

# Make directories
DISTDIR="rss-$VERSION"
rm -rf "$DISTDIR"
mkdir -p "${DISTDIR}/jars"
echo "RSS ${VERSION}${GITREVSTRING} built" >"${DISTDIR}/RELEASE"
echo "Build flags: $@" >>"$DISTDIR/RELEASE"
mkdir -p "${DISTDIR}/logs"

SERVER_JAR_DIR="${DISTDIR}/jars/server"
mkdir -p $SERVER_JAR_DIR
#SERVER_JAR="${RSS_HOME}/server/target/shuffle-server-${VERSION}-jar-with-dependencies.jar"
SERVER_JAR="${RSS_HOME}/server/target/shuffle-server-${VERSION}.jar"
echo "copy $SERVER_JAR to ${SERVER_JAR_DIR}"
cp $SERVER_JAR ${SERVER_JAR_DIR}
cp "${RSS_HOME}"/server/target/jars/* ${SERVER_JAR_DIR}

COORDINATOR_JAR_DIR="${DISTDIR}/jars/coordinator"
mkdir -p $COORDINATOR_JAR_DIR
#COORDINATOR_JAR="${RSS_HOME}/coordinator/target/coordinator-${VERSION}-jar-with-dependencies.jar"
COORDINATOR_JAR="${RSS_HOME}/coordinator/target/coordinator-${VERSION}.jar"
echo "copy $COORDINATOR_JAR to ${COORDINATOR_JAR_DIR}"
cp $COORDINATOR_JAR ${COORDINATOR_JAR_DIR}
cp "${RSS_HOME}"/coordinator/target/jars/* ${COORDINATOR_JAR_DIR}

CLIENT_JAR_DIR="${DISTDIR}/jars/client"
mkdir -p $CLIENT_JAR_DIR
CLIENT_JAR="${RSS_HOME}/client-spark2/target/rss-client-spark2-${VERSION}.jar"
echo "copy $CLIENT_JAR to ${CLIENT_JAR_DIR}"
cp $CLIENT_JAR ${CLIENT_JAR_DIR}

cp -r bin $DISTDIR
cp -r conf $DISTDIR

rm -rf "rss-$VERSION.tgz"
tar czf "rss-$VERSION.tgz" $DISTDIR
rm -rf $DISTDIR
