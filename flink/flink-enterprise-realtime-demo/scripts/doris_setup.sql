CREATE DATABASE IF NOT EXISTS rt_demo;
USE rt_demo;

DROP TABLE IF EXISTS ods_user_info;
CREATE TABLE ods_user_info (
    user_id BIGINT NOT NULL,
    user_name VARCHAR(64),
    user_level VARCHAR(32),
    city VARCHAR(64),
    register_time DATETIME,
    update_time DATETIME
) UNIQUE KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 3
PROPERTIES (
    'replication_num' = '1',
    'enable_unique_key_merge_on_write' = 'true'
);

DROP TABLE IF EXISTS ods_shop_info;
CREATE TABLE ods_shop_info (
    shop_id BIGINT NOT NULL,
    shop_name VARCHAR(64),
    shop_level VARCHAR(32),
    city VARCHAR(64),
    update_time DATETIME
) UNIQUE KEY(shop_id)
DISTRIBUTED BY HASH(shop_id) BUCKETS 3
PROPERTIES (
    'replication_num' = '1',
    'enable_unique_key_merge_on_write' = 'true'
);

DROP TABLE IF EXISTS ods_product_info;
CREATE TABLE ods_product_info (
    product_id BIGINT NOT NULL,
    product_name VARCHAR(64),
    category VARCHAR(32),
    price DECIMAL(10, 2),
    shop_id BIGINT,
    update_time DATETIME
) UNIQUE KEY(product_id)
DISTRIBUTED BY HASH(product_id) BUCKETS 3
PROPERTIES (
    'replication_num' = '1',
    'enable_unique_key_merge_on_write' = 'true'
);

DROP TABLE IF EXISTS ods_order_events;
CREATE TABLE ods_order_events (
    order_id BIGINT NOT NULL,
    user_id BIGINT,
    product_id BIGINT,
    quantity INT,
    order_amount DECIMAL(10, 2),
    order_status VARCHAR(32),
    order_time VARCHAR(32)
) UNIQUE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 3
PROPERTIES (
    'replication_num' = '1',
    'enable_unique_key_merge_on_write' = 'true'
);

DROP TABLE IF EXISTS ods_payment_events;
CREATE TABLE ods_payment_events (
    payment_id BIGINT NOT NULL,
    order_id BIGINT,
    user_id BIGINT,
    product_id BIGINT,
    shop_id BIGINT,
    payment_amount DECIMAL(10, 2),
    payment_status VARCHAR(32),
    payment_channel VARCHAR(32),
    payment_time VARCHAR(32)
) UNIQUE KEY(payment_id)
DISTRIBUTED BY HASH(payment_id) BUCKETS 3
PROPERTIES (
    'replication_num' = '1',
    'enable_unique_key_merge_on_write' = 'true'
);

DROP TABLE IF EXISTS ods_page_view_events;
CREATE TABLE ods_page_view_events (
    event_id BIGINT NOT NULL,
    user_id BIGINT,
    page_type VARCHAR(32),
    product_id BIGINT,
    shop_id BIGINT,
    stay_seconds INT,
    view_time VARCHAR(32)
) UNIQUE KEY(event_id)
DISTRIBUTED BY HASH(event_id) BUCKETS 3
PROPERTIES (
    'replication_num' = '1',
    'enable_unique_key_merge_on_write' = 'true'
);

DROP TABLE IF EXISTS dwd_order_wide;
CREATE TABLE dwd_order_wide (
    order_id BIGINT NOT NULL,
    user_id BIGINT,
    user_name VARCHAR(64),
    user_level VARCHAR(32),
    user_city VARCHAR(64),
    product_id BIGINT,
    product_name VARCHAR(64),
    category VARCHAR(32),
    price DECIMAL(10, 2),
    shop_id BIGINT,
    shop_name VARCHAR(64),
    shop_level VARCHAR(32),
    shop_city VARCHAR(64),
    quantity INT,
    order_amount DECIMAL(10, 2),
    order_status VARCHAR(32),
    order_time VARCHAR(32),
    row_time DATETIME
) UNIQUE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 3
PROPERTIES (
    'replication_num' = '1',
    'enable_unique_key_merge_on_write' = 'true'
);

DROP TABLE IF EXISTS dws_trade_metrics_10s;
CREATE TABLE dws_trade_metrics_10s (
    window_start VARCHAR(32),
    window_end VARCHAR(32),
    order_cnt BIGINT,
    order_user_cnt BIGINT,
    order_gmv DECIMAL(18, 2)
) DUPLICATE KEY(window_start, window_end)
DISTRIBUTED BY HASH(window_start) BUCKETS 3
PROPERTIES ('replication_num' = '1');

DROP TABLE IF EXISTS dws_product_sales_10s;
CREATE TABLE dws_product_sales_10s (
    window_start VARCHAR(32),
    window_end VARCHAR(32),
    product_id BIGINT,
    product_name VARCHAR(64),
    order_cnt BIGINT,
    order_gmv DECIMAL(18, 2)
) DUPLICATE KEY(window_start, window_end, product_id)
DISTRIBUTED BY HASH(product_id) BUCKETS 3
PROPERTIES ('replication_num' = '1');

DROP TABLE IF EXISTS dws_shop_trade_10s;
CREATE TABLE dws_shop_trade_10s (
    window_start VARCHAR(32),
    window_end VARCHAR(32),
    shop_id BIGINT,
    shop_name VARCHAR(64),
    order_cnt BIGINT,
    order_gmv DECIMAL(18, 2)
) DUPLICATE KEY(window_start, window_end, shop_id)
DISTRIBUTED BY HASH(shop_id) BUCKETS 3
PROPERTIES ('replication_num' = '1');

DROP TABLE IF EXISTS alert_payment_failure_1m;
CREATE TABLE alert_payment_failure_1m (
    window_start VARCHAR(32),
    window_end VARCHAR(32),
    shop_id BIGINT,
    total_cnt BIGINT,
    fail_cnt BIGINT,
    fail_rate DECIMAL(10, 4),
    alert_type VARCHAR(64),
    alert_message VARCHAR(255)
) DUPLICATE KEY(window_start, window_end, shop_id)
DISTRIBUTED BY HASH(shop_id) BUCKETS 3
PROPERTIES ('replication_num' = '1');

DROP TABLE IF EXISTS alert_high_traffic_1m;
CREATE TABLE alert_high_traffic_1m (
    window_start VARCHAR(32),
    window_end VARCHAR(32),
    page_type VARCHAR(32),
    product_id BIGINT,
    shop_id BIGINT,
    pv_cnt BIGINT,
    alert_type VARCHAR(64),
    alert_message VARCHAR(255)
) DUPLICATE KEY(window_start, window_end, page_type, product_id, shop_id)
DISTRIBUTED BY HASH(page_type) BUCKETS 3
PROPERTIES ('replication_num' = '1');
