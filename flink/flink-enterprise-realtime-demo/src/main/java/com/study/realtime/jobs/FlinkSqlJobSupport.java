package com.study.realtime.jobs;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Shared helpers for local SQL jobs.
 */
public final class FlinkSqlJobSupport {

    private FlinkSqlJobSupport() {
    }

    public static StreamTableEnvironment createTableEnv(String jobName) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        TableConfig config = tableEnv.getConfig();
        config.getConfiguration().setString("pipeline.name", jobName);
        config.getConfiguration().setString("table.exec.sink.upsert-materialize", "NONE");
        config.getConfiguration().setString("execution.checkpointing.interval", "5 s");
        config.getConfiguration().setString("table.local-time-zone", "Asia/Shanghai");
        return tableEnv;
    }

    public static void executeSql(StreamTableEnvironment tableEnv, String sql) {
        tableEnv.executeSql(sql);
    }
}
