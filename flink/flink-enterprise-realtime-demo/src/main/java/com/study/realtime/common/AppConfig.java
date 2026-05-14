package com.study.realtime.common;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 统一的应用配置加载类。
 *
 * 主要目标：
 * 1. 先读取项目内置的默认配置文件 application.properties。
 * 2. 如果启动参数里指定了外部配置文件，则用外部配置覆盖默认配置。
 * 3. 如果又传入了 -Ddemo.xxx 或 -Dflink.xxx 这类 JVM 参数，则再做最终覆盖。
 *
 * 最终优先级：
 * JVM 参数 > 外部配置文件 > Jar 内默认配置。
 *
 * 这样做的好处是：
 * - 开发环境可以直接使用项目自带默认配置。
 * - 测试、生产环境可以通过外部配置文件切换环境，不需要改代码。
 * - 某些临时参数也可以通过 -D 参数直接覆盖，方便调试和发布。
 */
public final class AppConfig {

    // Jar 包内默认配置文件名，放在 src/main/resources 下。
    private static final String CLASSPATH_CONFIG = "application.properties";
    // 外部配置文件的 JVM 参数名，例如：-Ddemo.config=/path/to/app-prod.properties
    private static final String EXTERNAL_CONFIG_KEY = "demo.config";

    // 合并后的最终配置集合。
    private final Properties properties;

    private AppConfig(Properties properties) {
        this.properties = properties;
    }

    /**
     * 加载最终配置入口。
     *
     * 加载顺序：
     * 1. 先加载 classpath 默认配置
     * 2. 再加载外部配置文件覆盖
     * 3. 最后加载 JVM System Properties 覆盖
     */
    public static AppConfig load() {
        Properties merged = new Properties();
        loadClasspathDefaults(merged);
        loadExternalOverrides(merged);
        loadSystemOverrides(merged);
        return new AppConfig(merged);
    }

    /**
     * 读取一个必填配置。
     *
     * 如果配置不存在或为空，直接抛异常，让程序在启动阶段失败。
     * 这种方式适合读取数据库地址、账号、主题名这类关键配置，
     * 避免任务跑起来后才因为缺配置出问题。
     */
    public String getRequired(String key) {
        String value = properties.getProperty(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing required config: " + key);
        }
        return value.trim();
    }

    /**
     * 读取字符串配置。
     *
     * 如果没配，则返回传入的默认值。
     */
    public String get(String key, String defaultValue) {
        String value = properties.getProperty(key);
        return value == null || value.trim().isEmpty() ? defaultValue : value.trim();
    }

    /**
     * 按 int 类型读取配置。
     *
     * 适用于并行度、重启次数等整数型配置。
     */
    public int getInt(String key, int defaultValue) {
        return Integer.parseInt(get(key, String.valueOf(defaultValue)));
    }

    /**
     * 按 long 类型读取配置。
     *
     * 适用于 checkpoint 间隔、超时时间等毫秒级配置。
     */
    public long getLong(String key, long defaultValue) {
        return Long.parseLong(get(key, String.valueOf(defaultValue)));
    }

    /**
     * 按 boolean 类型读取配置。
     *
     * 适用于开关类配置，比如是否开启 externalized checkpoint。
     */
    public boolean getBoolean(String key, boolean defaultValue) {
        return Boolean.parseBoolean(get(key, String.valueOf(defaultValue)));
    }

    /**
     * 加载 Jar 包内默认配置。
     *
     * 这里读取的是 resources 目录下的 application.properties。
     * 如果文件存在，就先作为第一层默认配置装载进来。
     */
    private static void loadClasspathDefaults(Properties merged) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try (InputStream inputStream = classLoader.getResourceAsStream(CLASSPATH_CONFIG)) {
            if (inputStream != null) {
                merged.load(inputStream);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load classpath config: " + CLASSPATH_CONFIG, e);
        }
    }

    /**
     * 加载外部配置文件。
     *
     * 例如启动时传入：
     * -Ddemo.config=/opt/module/flink-enterprise-realtime-demo/conf/application-prod.properties
     *
     * 如果传了这个参数，就读取该文件，并覆盖默认配置。
     */
    private static void loadExternalOverrides(Properties merged) {
        String externalConfigPath = System.getProperty(EXTERNAL_CONFIG_KEY);
        if (externalConfigPath == null || externalConfigPath.trim().isEmpty()) {
            return;
        }

        try (InputStream inputStream = new FileInputStream(externalConfigPath.trim())) {
            merged.load(inputStream);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load external config: " + externalConfigPath, e);
        }
    }

    /**
     * 加载 JVM 启动参数中的配置项作为最终覆盖层。
     *
     * 这里只接收以 demo. 或 flink. 开头的参数，避免把无关的系统属性全部混进来。
     *
     * 例如：
     * -Dflink.parallelism=4
     * -Ddemo.kafka.bootstrap-servers=xxx:9092
     *
     * 这些值会覆盖前面 classpath 和外部配置文件中的同名配置。
     */
    private static void loadSystemOverrides(Properties merged) {
        Properties systemProperties = System.getProperties();
        for (String key : systemProperties.stringPropertyNames()) {
            if (key.startsWith("demo.") || key.startsWith("flink.")) {
                merged.setProperty(key, systemProperties.getProperty(key));
            }
        }
    }
}
