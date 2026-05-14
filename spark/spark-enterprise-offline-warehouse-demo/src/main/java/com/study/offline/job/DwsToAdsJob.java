package com.study.offline.job;

import com.study.offline.common.AppConfig;
import com.study.offline.common.SparkSessionFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class DwsToAdsJob extends BaseJob {

    public static void main(String[] args) {
        AppConfig config = AppConfig.load();
        DwsToAdsJob job = new DwsToAdsJob();
        String bizDate = job.getBizDate(config, args);
        SparkSession spark = null;
        try {
            spark = SparkSessionFactory.create(config, "offline-dws-to-ads");
            run(spark, config, bizDate, job.getWarehouseBasePath(config));
        } finally {
            job.stopQuietly(spark);
        }
    }

    public static void run(SparkSession spark, AppConfig config, String bizDate, String basePath) {
        spark.read()
                .parquet(basePath + "/dws/dws_trade_day_summary/dt=" + bizDate)
                .withColumn("dt", functions.lit(bizDate))
                .createOrReplaceTempView("dws_trade_day_summary");
        spark.read()
                .parquet(basePath + "/dws/dws_shop_day_summary/dt=" + bizDate)
                .withColumn("dt", functions.lit(bizDate))
                .createOrReplaceTempView("dws_shop_day_summary");
        spark.read()
                .parquet(basePath + "/dws/dws_product_day_summary/dt=" + bizDate)
                .withColumn("dt", functions.lit(bizDate))
                .createOrReplaceTempView("dws_product_day_summary");
        spark.read()
                .parquet(basePath + "/dws/dws_user_day_summary/dt=" + bizDate)
                .withColumn("dt", functions.lit(bizDate))
                .createOrReplaceTempView("dws_user_day_summary");

        spark.sql(
                "select "
                        + "dt, "
                        + "order_cnt, "
                        + "order_user_cnt, "
                        + "order_amount, "
                        + "paid_order_amount, "
                        + "case when order_cnt = 0 then 0 else round(order_amount / order_cnt, 2) end as avg_order_amount "
                        + "from dws_trade_day_summary"
        ).write().mode("overwrite").partitionBy("dt").parquet(basePath + "/ads/ads_trade_overview");

        spark.sql(
                "select * from ("
                        + "select dt, product_id, product_name, category_name, sale_quantity, sale_amount, "
                        + "row_number() over(order by sale_amount desc, sale_quantity desc, product_id asc) as rn "
                        + "from dws_product_day_summary"
                        + ") t where rn <= 10"
        ).write().mode("overwrite").partitionBy("dt").parquet(basePath + "/ads/ads_top10_products");

        spark.sql(
                "select * from ("
                        + "select dt, shop_id, shop_name, order_cnt, order_amount, sale_quantity, "
                        + "row_number() over(order by order_amount desc, sale_quantity desc, shop_id asc) as rn "
                        + "from dws_shop_day_summary"
                        + ") t where rn <= 10"
        ).write().mode("overwrite").partitionBy("dt").parquet(basePath + "/ads/ads_top10_shops");

        spark.sql(
                "select "
                        + "dt, "
                        + "case when user_level in ('VIP', 'SVIP') then 'OLD_USER' else 'NEW_USER' end as user_type, "
                        + "count(1) as user_cnt, "
                        + "sum(order_amount) as order_amount "
                        + "from dws_user_day_summary "
                        + "group by dt, case when user_level in ('VIP', 'SVIP') then 'OLD_USER' else 'NEW_USER' end"
        ).write().mode("overwrite").partitionBy("dt").parquet(basePath + "/ads/ads_user_growth_analysis");
    }
}
