package com.study.offline.common;

import org.apache.spark.sql.SparkSession;

public class SparkSessionFactory {

    private SparkSessionFactory() {
    }

    public static SparkSession create(AppConfig config, String appName) {
        SparkSession.Builder builder = SparkSession.builder()
                .appName(appName)
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                .config("spark.sql.session.timeZone", config.getOrDefault("demo.mysql.serverTimezone", "Asia/Shanghai"))
                .config("spark.sql.shuffle.partitions", config.getOrDefault("spark.sql.shuffle.partitions", "4"))
                .config("spark.default.parallelism", config.getOrDefault("spark.default.parallelism", "4"));

        String runMode = config.getOrDefault("demo.runMode", "local");
        if ("local".equalsIgnoreCase(runMode)) {
            builder.master("local[*]");
            builder.config("spark.driver.memory", config.getOrDefault("spark.driver.memory", "1g"));
        }
        return builder.getOrCreate();
    }
}
