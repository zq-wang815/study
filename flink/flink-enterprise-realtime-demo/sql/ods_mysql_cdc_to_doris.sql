SET 'pipeline.name' = 'rt-demo-ods-mysql-cdc';
SET 'execution.checkpointing.interval' = '5 s';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'table.local-time-zone' = 'Asia/Shanghai';

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
    'server-id' = '6101-6104',
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
    'server-id' = '6201-6204',
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
    'server-id' = '6301-6304',
    'server-time-zone' = 'Asia/Shanghai',
    'scan.startup.mode' = 'initial'
);

CREATE TABLE ods_user_info (
    user_id BIGINT,
    user_name STRING,
    user_level STRING,
    city STRING,
    register_time TIMESTAMP(3),
    update_time TIMESTAMP(3)
) WITH (
    'connector' = 'doris',
    'fenodes' = '47.103.24.91:18030',
    'benodes' = '47.103.24.91:18040',
    'table.identifier' = 'rt_demo.ods_user_info',
    'username' = 'root',
    'password' = 'Wzq19940920..',
    'sink.label-prefix' = 'ods_user_info_sql_20260514',
    'sink.enable-2pc' = 'true',
    'sink.enable-delete' = 'true',
    'sink.properties.format' = 'json',
    'sink.properties.read_json_by_line' = 'true'
);

CREATE TABLE ods_product_info (
    product_id BIGINT,
    product_name STRING,
    category STRING,
    price DECIMAL(10, 2),
    shop_id BIGINT,
    update_time TIMESTAMP(3)
) WITH (
    'connector' = 'doris',
    'fenodes' = '47.103.24.91:18030',
    'benodes' = '47.103.24.91:18040',
    'table.identifier' = 'rt_demo.ods_product_info',
    'username' = 'root',
    'password' = 'Wzq19940920..',
    'sink.label-prefix' = 'ods_product_info_sql_20260514',
    'sink.enable-2pc' = 'true',
    'sink.enable-delete' = 'true',
    'sink.properties.format' = 'json',
    'sink.properties.read_json_by_line' = 'true'
);

CREATE TABLE ods_shop_info (
    shop_id BIGINT,
    shop_name STRING,
    shop_level STRING,
    city STRING,
    update_time TIMESTAMP(3)
) WITH (
    'connector' = 'doris',
    'fenodes' = '47.103.24.91:18030',
    'benodes' = '47.103.24.91:18040',
    'table.identifier' = 'rt_demo.ods_shop_info',
    'username' = 'root',
    'password' = 'Wzq19940920..',
    'sink.label-prefix' = 'ods_shop_info_sql_20260514',
    'sink.enable-2pc' = 'true',
    'sink.enable-delete' = 'true',
    'sink.properties.format' = 'json',
    'sink.properties.read_json_by_line' = 'true'
);

BEGIN STATEMENT SET;

INSERT INTO ods_user_info SELECT * FROM mysql_user_info;
INSERT INTO ods_product_info SELECT * FROM mysql_product_info;
INSERT INTO ods_shop_info SELECT * FROM mysql_shop_info;

END;
