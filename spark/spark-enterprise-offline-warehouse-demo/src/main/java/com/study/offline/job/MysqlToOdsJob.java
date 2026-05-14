package com.study.offline.job;

import com.study.offline.common.AppConfig;
import com.study.offline.common.JdbcOptions;
import com.study.offline.common.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class MysqlToOdsJob extends BaseJob {

    public static void main(String[] args) {
        AppConfig config = AppConfig.load();
        MysqlToOdsJob job = new MysqlToOdsJob();
        String bizDate = job.getBizDate(config, args);
        SparkSession spark = null;
        try {
            spark = SparkSessionFactory.create(config, "offline-mysql-to-ods");
            run(spark, config, bizDate, job.getWarehouseBasePath(config));
        } finally {
            job.stopQuietly(spark);
        }
    }

    public static void run(SparkSession spark, AppConfig config, String bizDate, String basePath) {
        String jdbcUrl = JdbcOptions.buildJdbcUrl(config);

        writeTable(spark, jdbcUrl, config, "user_info", basePath + "/ods/ods_user_info", bizDate);
        writeTable(spark, jdbcUrl, config, "product_info", basePath + "/ods/ods_product_info", bizDate);
        writeTable(spark, jdbcUrl, config, "shop_info", basePath + "/ods/ods_shop_info", bizDate);
        writeTable(spark, jdbcUrl, config, "order_info", basePath + "/ods/ods_order_info", bizDate);
        writeTable(spark, jdbcUrl, config, "order_item", basePath + "/ods/ods_order_item", bizDate);
        writeTable(spark, jdbcUrl, config, "payment_info", basePath + "/ods/ods_payment_info", bizDate);
    }

    private static void writeTable(SparkSession spark,
                                   String jdbcUrl,
                                   AppConfig config,
                                   String sourceTable,
                                   String targetPath,
                                   String bizDate) {
        Dataset<Row> source = spark.read()
                .jdbc(jdbcUrl, sourceTable, JdbcOptions.buildProperties(config))
                .withColumn("dt", functions.lit(bizDate))
                .withColumn("etl_time", functions.current_timestamp());

        source.write()
                .mode(SaveMode.Overwrite)
                .partitionBy("dt")
                .parquet(targetPath);
    }
}
