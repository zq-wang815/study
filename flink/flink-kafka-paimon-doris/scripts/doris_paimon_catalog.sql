-- ============================================================
-- Doris: Create Paimon external catalog to query Paimon tables
-- Run in Doris: mysql -h 172.31.249.211 -P 9030 -u root -p
-- ============================================================

-- 1. Create Paimon catalog pointing to HDFS warehouse
-- (Paimon tables are written by Flink to HDFS)
CREATE CATALOG IF NOT EXISTS paimon_catalog PROPERTIES (
    "type" = "paimon",
    "paimon.catalog.type" = "filesystem",
    "warehouse" = "hdfs://localhost:9000/paimon"
);

-- 2. Switch to Paimon catalog and check tables
SWITCH paimon_catalog;
SHOW DATABASES;

-- 3. Query the Paimon table (database: ods, table: orders)
USE ods;
SHOW TABLES;
SELECT * FROM orders LIMIT 10;
SELECT COUNT(*) AS total FROM orders;
