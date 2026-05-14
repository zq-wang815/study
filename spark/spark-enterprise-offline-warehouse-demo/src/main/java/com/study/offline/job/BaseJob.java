package com.study.offline.job;

import com.study.offline.common.AppConfig;
import org.apache.spark.sql.SparkSession;

public abstract class BaseJob {

    protected String getBizDate(AppConfig config, String[] args) {
        if (args != null && args.length > 0 && args[0] != null && !args[0].trim().isEmpty()) {
            return args[0].trim();
        }
        return config.getRequired("demo.bizDate");
    }

    protected String getWarehouseBasePath(AppConfig config) {
        String runMode = config.getOrDefault("demo.runMode", "local");
        if ("local".equalsIgnoreCase(runMode)) {
            return config.getRequired("demo.local.basePath");
        }
        return config.getRequired("demo.hdfs.basePath");
    }

    protected void stopQuietly(SparkSession spark) {
        if (spark != null) {
            spark.stop();
        }
    }
}
