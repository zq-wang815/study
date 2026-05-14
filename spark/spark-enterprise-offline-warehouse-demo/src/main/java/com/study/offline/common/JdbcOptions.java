package com.study.offline.common;

import java.util.Properties;

public class JdbcOptions {

    private JdbcOptions() {
    }

    public static String buildJdbcUrl(AppConfig config) {
        return "jdbc:mysql://"
                + config.getRequired("demo.mysql.host")
                + ":"
                + config.getRequired("demo.mysql.port")
                + "/"
                + config.getRequired("demo.mysql.database")
                + "?useSSL=false&allowPublicKeyRetrieval=true&characterEncoding=UTF-8&serverTimezone="
                + config.getOrDefault("demo.mysql.serverTimezone", "Asia/Shanghai");
    }

    public static Properties buildProperties(AppConfig config) {
        Properties properties = new Properties();
        properties.setProperty("driver", "com.mysql.cj.jdbc.Driver");
        properties.setProperty("user", config.getRequired("demo.mysql.username"));
        properties.setProperty("password", config.getRequired("demo.mysql.password"));
        properties.setProperty("fetchsize", "1000");
        return properties;
    }
}
