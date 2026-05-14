package com.study.offline.common;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AppConfig {
    private final Properties properties = new Properties();

    private AppConfig() {
    }

    public static AppConfig load() {
        AppConfig appConfig = new AppConfig();
        String externalConfig = System.getProperty("demo.config");
        try (InputStream inputStream = externalConfig != null
                ? new FileInputStream(externalConfig)
                : AppConfig.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (inputStream == null) {
                throw new IllegalStateException("Cannot load configuration file");
            }
            appConfig.properties.load(inputStream);
            return appConfig;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load configuration", e);
        }
    }

    public String getRequired(String key) {
        String systemValue = System.getProperty(key);
        if (systemValue != null && !systemValue.trim().isEmpty()) {
            return systemValue.trim();
        }
        String value = properties.getProperty(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing required config: " + key);
        }
        return value.trim();
    }

    public String getOrDefault(String key, String defaultValue) {
        String systemValue = System.getProperty(key);
        if (systemValue != null && !systemValue.trim().isEmpty()) {
            return systemValue.trim();
        }
        String value = properties.getProperty(key);
        return value == null || value.trim().isEmpty() ? defaultValue : value.trim();
    }

    public int getInt(String key, int defaultValue) {
        return Integer.parseInt(getOrDefault(key, String.valueOf(defaultValue)));
    }
}
