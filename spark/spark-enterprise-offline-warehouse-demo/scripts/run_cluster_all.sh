#!/usr/bin/env bash

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BIZ_DATE="${1:-2026-05-14}"
JAR_PATH="$BASE_DIR/target/spark-enterprise-offline-warehouse-demo-1.0-SNAPSHOT.jar"
CONF_PATH="$BASE_DIR/scripts/application-prod.properties"

COMMON_ARGS=(
  --master yarn
  --deploy-mode cluster
  --conf spark.driver.memory=512m
  --conf spark.executor.instances=1
  --conf spark.executor.cores=1
  --conf spark.executor.memory=512m
  --conf spark.yarn.am.memory=512m
  --conf "spark.driver.extraJavaOptions=-Ddemo.config=$CONF_PATH"
  --conf "spark.executor.extraJavaOptions=-Ddemo.config=$CONF_PATH"
)

/opt/module/spark/bin/spark-submit "${COMMON_ARGS[@]}" \
  --class com.study.offline.job.MysqlToOdsJob \
  "$JAR_PATH" \
  "$BIZ_DATE"

/opt/module/spark/bin/spark-submit "${COMMON_ARGS[@]}" \
  --class com.study.offline.job.OdsToDwdJob \
  "$JAR_PATH" \
  "$BIZ_DATE"

/opt/module/spark/bin/spark-submit "${COMMON_ARGS[@]}" \
  --class com.study.offline.job.DwdToDwsJob \
  "$JAR_PATH" \
  "$BIZ_DATE"

/opt/module/spark/bin/spark-submit "${COMMON_ARGS[@]}" \
  --class com.study.offline.job.DwsToAdsJob \
  "$JAR_PATH" \
  "$BIZ_DATE"
