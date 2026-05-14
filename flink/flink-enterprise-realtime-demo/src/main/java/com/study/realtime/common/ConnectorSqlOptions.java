package com.study.realtime.common;

/**
 * Shared SQL option builders for external connectors.
 */
public final class ConnectorSqlOptions {

    private ConnectorSqlOptions() {
    }

    public static String kafka(AppConfig config, String topic, String groupId) {
        return " 'connector' = 'kafka',"
                + " 'topic' = '" + topic + "',"
                + " 'properties.bootstrap.servers' = '" + config.getRequired("demo.kafka.bootstrap-servers") + "',"
                + " 'properties.group.id' = '" + groupId + "',"
                + " 'scan.startup.mode' = '" + config.get("demo.kafka.startup-mode", "earliest-offset") + "',"
                + " 'format' = 'json',"
                + " 'json.ignore-parse-errors' = 'true'";
    }

    public static String mysqlCdc(AppConfig config, String tableName, String serverId) {
        return " 'connector' = 'mysql-cdc',"
                + " 'hostname' = '" + config.getRequired("demo.mysql.host") + "',"
                + " 'port' = '" + config.getRequired("demo.mysql.port") + "',"
                + " 'username' = '" + config.getRequired("demo.mysql.username") + "',"
                + " 'password' = '" + config.getRequired("demo.mysql.password") + "',"
                + " 'database-name' = '" + config.getRequired("demo.mysql.database") + "',"
                + " 'table-name' = '" + tableName + "',"
                + " 'server-id' = '" + serverId + "',"
                + " 'server-time-zone' = '" + config.get("demo.mysql.server-time-zone", "Asia/Shanghai") + "',"
                + " 'scan.startup.mode' = '" + config.get("demo.mysql.cdc.startup-mode", "initial") + "'";
    }

    public static String mysqlJdbcLookup(AppConfig config, String tableName) {
        return " 'connector' = 'jdbc',"
                + " 'url' = '" + mysqlJdbcUrl(config) + "',"
                + " 'table-name' = '" + tableName + "',"
                + " 'username' = '" + config.getRequired("demo.mysql.username") + "',"
                + " 'password' = '" + config.getRequired("demo.mysql.password") + "',"
                + " 'lookup.cache.max-rows' = '" + config.get("demo.mysql.lookup.cache.max-rows", "1000") + "',"
                + " 'lookup.cache.ttl' = '" + config.get("demo.mysql.lookup.cache.ttl", "10 min") + "'";
    }

    public static String doris(AppConfig config, String tableName, String labelPrefix) {
        return " 'connector' = 'doris',"
                + " 'fenodes' = '" + config.getRequired("demo.doris.fe-nodes") + "',"
                + " 'benodes' = '" + config.getRequired("demo.doris.be-nodes") + "',"
                + " 'table.identifier' = '" + config.getRequired("demo.doris.database") + "." + tableName + "',"
                + " 'username' = '" + config.getRequired("demo.doris.username") + "',"
                + " 'password' = '" + config.getRequired("demo.doris.password") + "',"
                + " 'sink.label-prefix' = '" + labelPrefix + System.currentTimeMillis() + "',"
                + " 'sink.enable-2pc' = '" + config.get("demo.doris.sink.enable-2pc", "true") + "',"
                + " 'sink.enable-delete' = '" + config.get("demo.doris.sink.enable-delete", "true") + "',"
                + " 'sink.properties.format' = 'json',"
                + " 'sink.properties.read_json_by_line' = 'true'";
    }

    private static String mysqlJdbcUrl(AppConfig config) {
        return "jdbc:mysql://"
                + config.getRequired("demo.mysql.host")
                + ":"
                + config.getRequired("demo.mysql.port")
                + "/"
                + config.getRequired("demo.mysql.database")
                + "?useSSL=false&serverTimezone="
                + config.get("demo.mysql.server-time-zone", "Asia/Shanghai")
                + "&characterEncoding=UTF-8";
    }
}
