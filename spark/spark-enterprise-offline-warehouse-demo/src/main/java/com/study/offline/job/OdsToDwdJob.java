package com.study.offline.job;

import com.study.offline.common.AppConfig;
import com.study.offline.common.SparkSessionFactory;
import org.apache.spark.sql.SparkSession;

public class OdsToDwdJob extends BaseJob {

    public static void main(String[] args) {
        AppConfig config = AppConfig.load();
        OdsToDwdJob job = new OdsToDwdJob();
        String bizDate = job.getBizDate(config, args);
        SparkSession spark = null;
        try {
            spark = SparkSessionFactory.create(config, "offline-ods-to-dwd");
            run(spark, config, bizDate, job.getWarehouseBasePath(config));
        } finally {
            job.stopQuietly(spark);
        }
    }

    public static void run(SparkSession spark, AppConfig config, String bizDate, String basePath) {
        spark.read().parquet(basePath + "/ods/ods_order_info/dt=" + bizDate).createOrReplaceTempView("ods_order_info");
        spark.read().parquet(basePath + "/ods/ods_order_item/dt=" + bizDate).createOrReplaceTempView("ods_order_item");
        spark.read().parquet(basePath + "/ods/ods_user_info/dt=" + bizDate).createOrReplaceTempView("ods_user_info");
        spark.read().parquet(basePath + "/ods/ods_product_info/dt=" + bizDate).createOrReplaceTempView("ods_product_info");
        spark.read().parquet(basePath + "/ods/ods_shop_info/dt=" + bizDate).createOrReplaceTempView("ods_shop_info");
        spark.read().parquet(basePath + "/ods/ods_payment_info/dt=" + bizDate).createOrReplaceTempView("ods_payment_info");

        spark.sql(
                "select "
                        + "oi.id as order_item_id, "
                        + "oi.order_id, "
                        + "o.user_id, "
                        + "u.user_name, "
                        + "u.user_level, "
                        + "u.province, "
                        + "oi.product_id, "
                        + "p.product_name, "
                        + "p.category_name, "
                        + "p.shop_id, "
                        + "s.shop_name, "
                        + "s.shop_level, "
                        + "oi.quantity, "
                        + "oi.sale_price, "
                        + "oi.quantity * oi.sale_price as item_origin_amount, "
                        + "oi.quantity * oi.final_price as item_final_amount, "
                        + "oi.coupon_amount, "
                        + "o.order_status, "
                        + "o.order_time, "
                        + "o.pay_time, "
                        + "'" + bizDate + "' as dt "
                        + "from ods_order_item oi "
                        + "join ods_order_info o on oi.order_id = o.id "
                        + "left join ods_user_info u on o.user_id = u.id "
                        + "left join ods_product_info p on oi.product_id = p.id "
                        + "left join ods_shop_info s on p.shop_id = s.id"
        ).write().mode("overwrite").partitionBy("dt").parquet(basePath + "/dwd/dwd_order_detail");

        spark.sql(
                "select "
                        + "p.id as payment_id, "
                        + "p.order_id, "
                        + "o.user_id, "
                        + "p.payment_type, "
                        + "p.payment_status, "
                        + "p.payment_amount, "
                        + "p.callback_time, "
                        + "o.order_status, "
                        + "'" + bizDate + "' as dt "
                        + "from ods_payment_info p "
                        + "left join ods_order_info o on p.order_id = o.id"
        ).write().mode("overwrite").partitionBy("dt").parquet(basePath + "/dwd/dwd_payment_detail");
    }
}
