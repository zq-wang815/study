#!/usr/bin/env bash

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BIZ_DATE="${1:-2026-05-14}"
JAVA_8_HOME="${JAVA_8_HOME:-$(/usr/libexec/java_home -v 1.8)}"
CLASSPATH_FILE="${BASE_DIR}/target/local.classpath"

export JAVA_HOME="$JAVA_8_HOME"
export PATH="$JAVA_HOME/bin:$PATH"

rm -rf "$BASE_DIR/.local-warehouse"

cd "$BASE_DIR"

mvn -q -DskipTests dependency:build-classpath -Dmdep.outputFile="$CLASSPATH_FILE" -Dmdep.includeScope=runtime

CP="$BASE_DIR/target/classes:$(cat "$CLASSPATH_FILE")"

java -cp "$CP" com.study.offline.job.MysqlToOdsJob "$BIZ_DATE"
java -cp "$CP" com.study.offline.job.OdsToDwdJob "$BIZ_DATE"
java -cp "$CP" com.study.offline.job.DwdToDwsJob "$BIZ_DATE"
java -cp "$CP" com.study.offline.job.DwsToAdsJob "$BIZ_DATE"
