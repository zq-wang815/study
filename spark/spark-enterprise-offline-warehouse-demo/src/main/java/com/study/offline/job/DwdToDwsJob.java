package com.study.offline.job;

import com.study.offline.common.AppConfig;
import com.study.offline.common.SparkSessionFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class DwdToDwsJob extends BaseJob {

    public static void main(String[] args) {
        AppConfig config = AppConfig.load();
        DwdToDwsJob job = new DwdToDwsJob();
        String bizDate = job.getBizDate(config, args);
        SparkSession spark = null;
        try {
            spark = SparkSessionFactory.create(config, "offline-dwd-to-dws");
            run(spark, config, bizDate, job.getWarehouseBasePath(config));
        } finally {
            job.stopQuietly(spark);
        }
    }

    public static void run(SparkSession spark, AppConfig config, String bizDate, String basePath) {
        spark.read()
                .parquet(basePath + "/dwd/dwd_order_detail/dt=" + bizDate)
                .withColumn("dt", functions.lit(bizDate))
                .createOrReplaceTempView("dwd_order_detail");
        spark.read()
                .parquet(basePath + "/dwd/dwd_payment_detail/dt=" + bizDate)
                .withColumn("dt", functions.lit(bizDate))
                .createOrReplaceTempView("dwd_payment_detail");

        spark.sql(
                "select "
                        + "dt, "
                        + "count(distinct order_id) as order_cnt, "
                        + "count(distinct user_id) as order_user_cnt, "
                        + "sum(item_final_amount) as order_amount, "
                        + "sum(case when order_status = 'PAID' then item_final_amount else 0 end) as paid_order_amount "
                        + "from dwd_order_detail "
                        + "group by dt"
        ).write().mode("overwrite").partitionBy("dt").parquet(basePath + "/dws/dws_trade_day_summary");

        spark.sql(
                "select "
                        + "dt, "
                        + "shop_id, "
                        + "shop_name, "
                        + "count(distinct order_id) as order_cnt, "
                        + "sum(item_final_amount) as order_amount, "
                        + "sum(quantity) as sale_quantity "
                        + "from dwd_order_detail "
                        + "group by dt, shop_id, shop_name"
        ).write().mode("overwrite").partitionBy("dt").parquet(basePath + "/dws/dws_shop_day_summary");

        spark.sql(
                "select "
                        + "dt, "
                        + "product_id, "
                        + "product_name, "
                        + "category_name, "
                        + "sum(quantity) as sale_quantity, "
                        + "sum(item_final_amount) as sale_amount "
                        + "from dwd_order_detail "
                        + "group by dt, product_id, product_name, category_name"
        ).write().mode("overwrite").partitionBy("dt").parquet(basePath + "/dws/dws_product_day_summary");

        spark.sql(
                "select "
                        + "dt, "
                        + "user_id, "
                        + "user_name, "
                        + "user_level, "
                        + "province, "
                        + "count(distinct order_id) as order_cnt, "
                        + "sum(item_final_amount) as order_amount "
                        + "from dwd_order_detail "
                        + "group by dt, user_id, user_name, user_level, province"
        ).write().mode("overwrite").partitionBy("dt").parquet(basePath + "/dws/dws_user_day_summary");
    }
}
