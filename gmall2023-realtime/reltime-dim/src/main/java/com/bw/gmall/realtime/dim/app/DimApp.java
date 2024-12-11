package com.bw.gmall.realtime.dim.app;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DimApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // enable checkpoint
        env.enableCheckpointing(3000);
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall_config") // set captured database
                .tableList("gmall_config.table_process_dim") // set captured table
                .username("root")
                .password("000000")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .print(); // use parallelism 1 for sink to keep message ordering
    env.execute();

    }
}
