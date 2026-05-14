package com.study.offline.job;

import com.study.offline.common.AppConfig;
import com.study.offline.common.SparkSessionFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class LocalVerificationRunner extends BaseJob {

    public static void main(String[] args) {
        AppConfig config = AppConfig.load();
        LocalVerificationRunner runner = new LocalVerificationRunner();
        String bizDate = runner.getBizDate(config, args);
        SparkSession spark = null;
        try {
            spark = SparkSessionFactory.create(config, "offline-local-verification");
            String basePath = runner.getWarehouseBasePath(config);

            List<String> tables = Arrays.asList(
                    "ods/ods_user_info",
                    "ods/ods_product_info",
                    "ods/ods_shop_info",
                    "ods/ods_order_info",
                    "ods/ods_order_item",
                    "ods/ods_payment_info",
                    "dwd/dwd_order_detail",
                    "dwd/dwd_payment_detail",
                    "dws/dws_trade_day_summary",
                    "dws/dws_shop_day_summary",
                    "dws/dws_product_day_summary",
                    "dws/dws_user_day_summary",
                    "ads/ads_trade_overview",
                    "ads/ads_top10_products",
                    "ads/ads_top10_shops",
                    "ads/ads_user_growth_analysis"
            );

            for (String table : tables) {
                long count = spark.read().parquet(basePath + "/" + table + "/dt=" + bizDate).count();
                System.out.println(table + "\t" + count);
            }

            Row tradeOverview = spark.read().parquet(basePath + "/ads/ads_trade_overview/dt=" + bizDate).head();
            System.out.println("ads_trade_overview_sample\t" + tradeOverview);
        } finally {
            runner.stopQuietly(spark);
        }
    }
}
