SET 'pipeline.name' = 'rt-demo-main-pipeline';
SET 'execution.checkpointing.interval' = '5 s';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'table.local-time-zone' = 'Asia/Shanghai';

CREATE TABLE order_events (
    order_id BIGINT,
    user_id BIGINT,
    product_id BIGINT,
    quantity INT,
    order_amount DECIMAL(10, 2),
    order_status STRING,
    order_time STRING,
    row_time AS TO_TIMESTAMP(order_time),
    pt AS PROCTIME(),
    WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'rt_order_events',
    'properties.bootstrap.servers' = '47.103.24.91:9092',
    'properties.group.id' = 'rt-order-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE payment_events (
    payment_id BIGINT,
    order_id BIGINT,
    user_id BIGINT,
    product_id BIGINT,
    shop_id BIGINT,
    payment_amount DECIMAL(10, 2),
    payment_status STRING,
    payment_channel STRING,
    payment_time STRING,
    row_time AS TO_TIMESTAMP(payment_time),
    WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'rt_payment_events',
    'properties.bootstrap.servers' = '47.103.24.91:9092',
    'properties.group.id' = 'rt-payment-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE page_view_events (
    event_id BIGINT,
    user_id BIGINT,
    page_type STRING,
    product_id BIGINT,
    shop_id BIGINT,
    stay_seconds INT,
    view_time STRING,
    row_time AS TO_TIMESTAMP(view_time),
    WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'rt_page_view_events',
    'properties.bootstrap.servers' = '47.103.24.91:9092',
    'properties.group.id' = 'rt-pv-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE mysql_user_info (
    user_id BIGINT,
    user_name STRING,
    user_level STRING,
    city STRING,
    register_time TIMESTAMP(3),
    update_time TIMESTAMP(3),
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '47.103.24.91',
    'port' = '3306',
    'username' = 'root',
    'password' = 'Wzq19940920..',
    'database-name' = 'test',
    'table-name' = 'rt_user_info',
    'server-id' = '6401-6404',
    'server-time-zone' = 'Asia/Shanghai',
    'scan.startup.mode' = 'initial'
);

CREATE TABLE mysql_product_info (
    product_id BIGINT,
    product_name STRING,
    category STRING,
    price DECIMAL(10, 2),
    shop_id BIGINT,
    update_time TIMESTAMP(3),
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '47.103.24.91',
    'port' = '3306',
    'username' = 'root',
    'password' = 'Wzq19940920..',
    'database-name' = 'test',
    'table-name' = 'rt_product_info',
    'server-id' = '6501-6504',
    'server-time-zone' = 'Asia/Shanghai',
    'scan.startup.mode' = 'initial'
);

CREATE TABLE mysql_shop_info (
    shop_id BIGINT,
    shop_name STRING,
    shop_level STRING,
    city STRING,
    update_time TIMESTAMP(3),
    PRIMARY KEY (shop_id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '47.103.24.91',
    'port' = '3306',
    'username' = 'root',
    'password' = 'Wzq19940920..',
    'database-name' = 'test',
    'table-name' = 'rt_shop_info',
    'server-id' = '6601-6604',
    'server-time-zone' = 'Asia/Shanghai',
    'scan.startup.mode' = 'initial'
);

CREATE TEMPORARY VIEW dwd_order_wide_view AS
SELECT
    o.order_id,
    o.user_id,
    u.user_name,
    u.user_level,
    u.city AS user_city,
    o.product_id,
    p.product_name,
    p.category,
    p.price,
    p.shop_id,
    s.shop_name,
    s.shop_level,
    s.city AS shop_city,
    o.quantity,
    o.order_amount,
    o.order_status,
    o.order_time,
    o.row_time
FROM order_events AS o
LEFT JOIN mysql_user_info FOR SYSTEM_TIME AS OF o.pt AS u
ON o.user_id = u.user_id
LEFT JOIN mysql_product_info FOR SYSTEM_TIME AS OF o.pt AS p
ON o.product_id = p.product_id
LEFT JOIN mysql_shop_info FOR SYSTEM_TIME AS OF o.pt AS s
ON p.shop_id = s.shop_id;

CREATE TABLE ods_order_events (
    order_id BIGINT,
    user_id BIGINT,
    product_id BIGINT,
    quantity INT,
    order_amount DECIMAL(10, 2),
    order_status STRING,
    order_time STRING
) WITH (
    'connector' = 'doris',
    'fenodes' = '47.103.24.91:18030',
    'benodes' = '47.103.24.91:18040',
    'table.identifier' = 'rt_demo.ods_order_events',
    'username' = 'root',
    'password' = 'Wzq19940920..',
    'sink.label-prefix' = 'ods_order_events_sql_20260514',
    'sink.enable-2pc' = 'true',
    'sink.enable-delete' = 'true',
    'sink.properties.format' = 'json',
    'sink.properties.read_json_by_line' = 'true'
);

CREATE TABLE ods_payment_events (
    payment_id BIGINT,
    order_id BIGINT,
    user_id BIGINT,
    product_id BIGINT,
    shop_id BIGINT,
    payment_amount DECIMAL(10, 2),
    payment_status STRING,
    payment_channel STRING,
    payment_time STRING
) WITH (
    'connector' = 'doris',
    'fenodes' = '47.103.24.91:18030',
    'benodes' = '47.103.24.91:18040',
    'table.identifier' = 'rt_demo.ods_payment_events',
    'username' = 'root',
    'password' = 'Wzq19940920..',
    'sink.label-prefix' = 'ods_payment_events_sql_20260514',
    'sink.enable-2pc' = 'true',
    'sink.enable-delete' = 'true',
    'sink.properties.format' = 'json',
    'sink.properties.read_json_by_line' = 'true'
);

CREATE TABLE ods_page_view_events (
    event_id BIGINT,
    user_id BIGINT,
    page_type STRING,
    product_id BIGINT,
    shop_id BIGINT,
    stay_seconds INT,
    view_time STRING
) WITH (
    'connector' = 'doris',
    'fenodes' = '47.103.24.91:18030',
    'benodes' = '47.103.24.91:18040',
    'table.identifier' = 'rt_demo.ods_page_view_events',
    'username' = 'root',
    'password' = 'Wzq19940920..',
    'sink.label-prefix' = 'ods_page_view_events_sql_20260514',
    'sink.enable-2pc' = 'true',
    'sink.enable-delete' = 'true',
    'sink.properties.format' = 'json',
    'sink.properties.read_json_by_line' = 'true'
);

CREATE TABLE dwd_order_wide (
    order_id BIGINT,
    user_id BIGINT,
    user_name STRING,
    user_level STRING,
    user_city STRING,
    product_id BIGINT,
    product_name STRING,
    category STRING,
    price DECIMAL(10, 2),
    shop_id BIGINT,
    shop_name STRING,
    shop_level STRING,
    shop_city STRING,
    quantity INT,
    order_amount DECIMAL(10, 2),
    order_status STRING,
    order_time STRING,
    row_time TIMESTAMP(3)
) WITH (
    'connector' = 'doris',
    'fenodes' = '47.103.24.91:18030',
    'benodes' = '47.103.24.91:18040',
    'table.identifier' = 'rt_demo.dwd_order_wide',
    'username' = 'root',
    'password' = 'Wzq19940920..',
    'sink.label-prefix' = 'dwd_order_wide_sql_20260514',
    'sink.enable-2pc' = 'true',
    'sink.enable-delete' = 'true',
    'sink.properties.format' = 'json',
    'sink.properties.read_json_by_line' = 'true'
);

CREATE TABLE dws_trade_metrics_10s (
    window_start STRING,
    window_end STRING,
    order_cnt BIGINT,
    order_user_cnt BIGINT,
    order_gmv DECIMAL(18, 2)
) WITH (
    'connector' = 'doris',
    'fenodes' = '47.103.24.91:18030',
    'benodes' = '47.103.24.91:18040',
    'table.identifier' = 'rt_demo.dws_trade_metrics_10s',
    'username' = 'root',
    'password' = 'Wzq19940920..',
    'sink.label-prefix' = 'dws_trade_metrics_10s_sql_20260514',
    'sink.enable-2pc' = 'true',
    'sink.properties.format' = 'json',
    'sink.properties.read_json_by_line' = 'true'
);

CREATE TABLE dws_product_sales_10s (
    window_start STRING,
    window_end STRING,
    product_id BIGINT,
    product_name STRING,
    order_cnt BIGINT,
    order_gmv DECIMAL(18, 2)
) WITH (
    'connector' = 'doris',
    'fenodes' = '47.103.24.91:18030',
    'benodes' = '47.103.24.91:18040',
    'table.identifier' = 'rt_demo.dws_product_sales_10s',
    'username' = 'root',
    'password' = 'Wzq19940920..',
    'sink.label-prefix' = 'dws_product_sales_10s_sql_20260514',
    'sink.enable-2pc' = 'true',
    'sink.properties.format' = 'json',
    'sink.properties.read_json_by_line' = 'true'
);

CREATE TABLE dws_shop_trade_10s (
    window_start STRING,
    window_end STRING,
    shop_id BIGINT,
    shop_name STRING,
    order_cnt BIGINT,
    order_gmv DECIMAL(18, 2)
) WITH (
    'connector' = 'doris',
    'fenodes' = '47.103.24.91:18030',
    'benodes' = '47.103.24.91:18040',
    'table.identifier' = 'rt_demo.dws_shop_trade_10s',
    'username' = 'root',
    'password' = 'Wzq19940920..',
    'sink.label-prefix' = 'dws_shop_trade_10s_sql_20260514',
    'sink.enable-2pc' = 'true',
    'sink.properties.format' = 'json',
    'sink.properties.read_json_by_line' = 'true'
);

CREATE TABLE alert_payment_failure_1m (
    window_start STRING,
    window_end STRING,
    shop_id BIGINT,
    total_cnt BIGINT,
    fail_cnt BIGINT,
    fail_rate DECIMAL(10, 4),
    alert_type STRING,
    alert_message STRING
) WITH (
    'connector' = 'doris',
    'fenodes' = '47.103.24.91:18030',
    'benodes' = '47.103.24.91:18040',
    'table.identifier' = 'rt_demo.alert_payment_failure_1m',
    'username' = 'root',
    'password' = 'Wzq19940920..',
    'sink.label-prefix' = 'alert_payment_failure_1m_sql_20260514',
    'sink.enable-2pc' = 'true',
    'sink.properties.format' = 'json',
    'sink.properties.read_json_by_line' = 'true'
);

CREATE TABLE alert_high_traffic_1m (
    window_start STRING,
    window_end STRING,
    page_type STRING,
    product_id BIGINT,
    shop_id BIGINT,
    pv_cnt BIGINT,
    alert_type STRING,
    alert_message STRING
) WITH (
    'connector' = 'doris',
    'fenodes' = '47.103.24.91:18030',
    'benodes' = '47.103.24.91:18040',
    'table.identifier' = 'rt_demo.alert_high_traffic_1m',
    'username' = 'root',
    'password' = 'Wzq19940920..',
    'sink.label-prefix' = 'alert_high_traffic_1m_sql_20260514',
    'sink.enable-2pc' = 'true',
    'sink.properties.format' = 'json',
    'sink.properties.read_json_by_line' = 'true'
);

BEGIN STATEMENT SET;

INSERT INTO ods_order_events
SELECT order_id, user_id, product_id, quantity, order_amount, order_status, order_time
FROM order_events;

INSERT INTO ods_payment_events
SELECT payment_id, order_id, user_id, product_id, shop_id, payment_amount, payment_status, payment_channel, payment_time
FROM payment_events;

INSERT INTO ods_page_view_events
SELECT event_id, user_id, page_type, product_id, shop_id, stay_seconds, view_time
FROM page_view_events;

INSERT INTO dwd_order_wide
SELECT * FROM dwd_order_wide_view;

INSERT INTO dws_trade_metrics_10s
SELECT
    DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS window_start,
    DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS window_end,
    COUNT(*) AS order_cnt,
    COUNT(DISTINCT user_id) AS order_user_cnt,
    CAST(SUM(order_amount) AS DECIMAL(18, 2)) AS order_gmv
FROM TABLE(TUMBLE(TABLE order_events, DESCRIPTOR(row_time), INTERVAL '10' SECONDS))
GROUP BY window_start, window_end;

INSERT INTO dws_product_sales_10s
SELECT
    DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS window_start,
    DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS window_end,
    product_id,
    product_name,
    COUNT(*) AS order_cnt,
    CAST(SUM(order_amount) AS DECIMAL(18, 2)) AS order_gmv
FROM TABLE(TUMBLE(TABLE dwd_order_wide_view, DESCRIPTOR(row_time), INTERVAL '10' SECONDS))
GROUP BY window_start, window_end, product_id, product_name;

INSERT INTO dws_shop_trade_10s
SELECT
    DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS window_start,
    DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS window_end,
    shop_id,
    shop_name,
    COUNT(*) AS order_cnt,
    CAST(SUM(order_amount) AS DECIMAL(18, 2)) AS order_gmv
FROM TABLE(TUMBLE(TABLE dwd_order_wide_view, DESCRIPTOR(row_time), INTERVAL '10' SECONDS))
GROUP BY window_start, window_end, shop_id, shop_name;

INSERT INTO alert_payment_failure_1m
SELECT
    DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS window_start,
    DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS window_end,
    shop_id,
    CAST(COUNT(*) AS BIGINT) AS total_cnt,
    CAST(SUM(CASE WHEN payment_status = 'FAILED' THEN 1 ELSE 0 END) AS BIGINT) AS fail_cnt,
    CAST(SUM(CASE WHEN payment_status = 'FAILED' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS DECIMAL(10, 4)) AS fail_rate,
    'PAYMENT_FAILURE_RATE_HIGH' AS alert_type,
    CONCAT('shop_id=', CAST(shop_id AS STRING), ', fail_rate=', CAST(CAST(SUM(CASE WHEN payment_status = 'FAILED' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS DECIMAL(10, 4)) AS STRING)) AS alert_message
FROM TABLE(TUMBLE(TABLE payment_events, DESCRIPTOR(row_time), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, shop_id
HAVING COUNT(*) >= 3 AND SUM(CASE WHEN payment_status = 'FAILED' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) >= 0.30;

INSERT INTO alert_high_traffic_1m
SELECT
    DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS window_start,
    DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS window_end,
    page_type,
    COALESCE(product_id, CAST(-1 AS BIGINT)) AS product_id,
    COALESCE(shop_id, CAST(-1 AS BIGINT)) AS shop_id,
    CAST(COUNT(*) AS BIGINT) AS pv_cnt,
    'HIGH_TRAFFIC' AS alert_type,
    CONCAT('page_type=', page_type, ', pv_cnt=', CAST(COUNT(*) AS STRING)) AS alert_message
FROM TABLE(TUMBLE(TABLE page_view_events, DESCRIPTOR(row_time), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, page_type, product_id, shop_id
HAVING COUNT(*) >= 6;

END;
