#!/usr/bin/env bash

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BIZ_DATE="${1:-2026-05-14}"

JAVA_8_HOME="${JAVA_8_HOME:-$(/usr/libexec/java_home -v 1.8)}"
export JAVA_HOME="$JAVA_8_HOME"

rm -rf "$BASE_DIR/.local-warehouse"

spark-submit \
  --class com.study.offline.job.MysqlToOdsJob \
  "$BASE_DIR/target/spark-enterprise-offline-warehouse-demo-1.0-SNAPSHOT.jar" \
  "$BIZ_DATE"

spark-submit \
  --class com.study.offline.job.OdsToDwdJob \
  "$BASE_DIR/target/spark-enterprise-offline-warehouse-demo-1.0-SNAPSHOT.jar" \
  "$BIZ_DATE"

spark-submit \
  --class com.study.offline.job.DwdToDwsJob \
  "$BASE_DIR/target/spark-enterprise-offline-warehouse-demo-1.0-SNAPSHOT.jar" \
  "$BIZ_DATE"

spark-submit \
  --class com.study.offline.job.DwsToAdsJob \
  "$BASE_DIR/target/spark-enterprise-offline-warehouse-demo-1.0-SNAPSHOT.jar" \
  "$BIZ_DATE"
