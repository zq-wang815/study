package com.study.realtime.jobs;

import com.study.realtime.common.AppConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Shared helpers for building production-style Flink SQL jobs.
 */
public final class FlinkSqlJobSupport {

    private FlinkSqlJobSupport() {
    }

    public static StreamTableEnvironment createTableEnv(String jobName, AppConfig appConfig) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureExecution(env, appConfig);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        TableConfig config = tableEnv.getConfig();
        config.getConfiguration().setString("pipeline.name", jobName);
        config.getConfiguration().setString(
                "table.exec.sink.upsert-materialize",
                appConfig.get("flink.table.exec.sink.upsert-materialize", "NONE"));
        config.getConfiguration().setString(
                "execution.checkpointing.interval",
                appConfig.getLong("flink.checkpoint.interval.ms", 5000L) + " ms");
        config.getConfiguration().setString(
                "table.local-time-zone",
                appConfig.get("flink.table.local-time-zone", "Asia/Shanghai"));
        return tableEnv;
    }

    public static void executeSql(StreamTableEnvironment tableEnv, String sql) {
        tableEnv.executeSql(sql);
    }

    private static void configureExecution(StreamExecutionEnvironment env, AppConfig appConfig) {
        long checkpointInterval = appConfig.getLong("flink.checkpoint.interval.ms", 5000L);
        CheckpointingMode checkpointingMode = CheckpointingMode.valueOf(
                appConfig.get("flink.checkpoint.mode", "EXACTLY_ONCE"));

        env.enableCheckpointing(checkpointInterval, checkpointingMode);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(
                appConfig.getLong("flink.checkpoint.min-pause.ms", 2000L));
        env.getCheckpointConfig().setCheckpointTimeout(
                appConfig.getLong("flink.checkpoint.timeout.ms", 60000L));
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(
                appConfig.getInt("flink.checkpoint.max-concurrent", 1));
        env.setParallelism(appConfig.getInt("flink.parallelism", 1));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                appConfig.getInt("flink.restart.attempts", 3),
                Time.milliseconds(appConfig.getLong("flink.restart.delay.ms", 10000L))));

        if (appConfig.getBoolean("flink.checkpoint.externalized.enabled", true)) {
            env.getCheckpointConfig().enableExternalizedCheckpoints(
                    CheckpointConfig.ExternalizedCheckpointCleanup.valueOf(
                            appConfig.get("flink.checkpoint.externalized.cleanup", "RETAIN_ON_CANCELLATION")));
        }
    }
}
