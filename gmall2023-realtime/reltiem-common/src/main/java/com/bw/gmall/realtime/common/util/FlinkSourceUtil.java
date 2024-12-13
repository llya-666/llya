package com.bw.gmall.realtime.common.util;

import com.bw.gmall.realtime.common.constant.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;
import java.util.Properties;

public class FlinkSourceUtil {
    public static KafkaSource<String> getKafkaSource(String topic,String groupId){
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(topic)
                .setGroupId(groupId)
//                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                // Start from latest offset
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if(message !=null){
                            return new String(message);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();
        return kafkaSource;
    }

    //获取MySqlSouorce
    public static MySqlSource<String> getMySqlSource(String database,String tableName){
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .databaseList(database) // monitor all tables under inventory database
                .tableList(database +"."+tableName)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .jdbcProperties(props)
                .build();
        return mySqlSource;
    }
}
