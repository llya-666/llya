package com.bw.gmall.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bw.gmall.realtime.common.bean.TableProcessDim;
import com.bw.gmall.realtime.common.constant.Constant;
import com.bw.gmall.realtime.common.util.HBaseutill;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import netscape.javascript.JSObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;


import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.bw.gmall.realtime.common.util.TableProcessFunction.deleteNotNeedColumns;

/**
 *
 */
public class DimApp {
    public static void main(String[] args) throws Exception {
        // TODO: 2024/12/12 基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(4);
        // TODO: 2024/12/12 检查点相关的设置
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //设置检查点的超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置两个检查点之间最小的时间间隔
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //设置重启策略
        // env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3,Time.days(30),Time.seconds(3)));
        //设置状态后端
        env.setStateBackend(new HashMapStateBackend());
//       env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");
        //设置用户
//        System.setProperty("HADOOP_USER_NAME","root");
        // TODO: 2024/12/12 读取kafka数据
        String groupId="dim_app_group_1";

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(Constant.TOPIC_DB)
                .setGroupId(groupId)
                //生产环境中,为了保证消费的精准一次性,手动维护偏移量
                //setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setStartingOffsets(OffsetsInitializer.latest())
                //.setValueOnlyDeserializer(new SimpleStringSchema())
                .setValueOnlyDeserializer(
                        new DeserializationSchema<String>() {
                            @Override
                            public String deserialize(byte[] message) throws IOException {
                                if(message != null){
                                    return new String(message);
                                }
                                return null;
                            }

                            @Override
                            public boolean isEndOfStream(String nextElement) {
                                return false;
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return TypeInformation.of(String.class);
                            }
                        }
                )
                .build();
        //消费数据 封装为流
        DataStreamSource<String> kafkaDs
                = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka_Source");
        // TODO: 2024/12/12 对业务流中数据类型进行转换并进行简单的etl
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDs.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr,
                                               ProcessFunction<String, JSONObject>.Context context,
                                               Collector<JSONObject> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String db = jsonObj.getString("database");
                        String type = jsonObj.getString("type");
                        String data = jsonObj.getString("data");
                        if ("gmall".equals(db)
                                && ("insert".equals(type)
                                || "update".equals(type)
                                || "delete".equals(type)
                                || "bootstrap-insert".equals(type))
                                && data != null
                                && data.length() > 2) {

                        }
                        {
                            out.collect(jsonObj);
                        }

                    }
                }
        );

//        jsonObjDS.print();

        // TODO: 2024/12/12 使用flinkCDC读取杯子表中的配置信息
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process_dim")
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .jdbcProperties(props)
                .build();
        //读取数据 封装为流
        DataStreamSource<String> mysqlStrDS = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(1);
        mysqlStrDS.print();

        // TODO: 2024/12/12 对配置流中的数据类型进行转换 jsonStr->实体类对象
        SingleOutputStreamOperator<TableProcessDim> tpDs = mysqlStrDS.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String jsonStr) throws Exception {
                        //为了处理方便,先转换成jsonObj
                        JSONObject jsonObject = JSON.parseObject(jsonStr);
                        String op = jsonObject.getString("op");
                        TableProcessDim tableProcessDim = null;
                        if ("d".equals(op)) {
                            //对配置表进行了一次删除操作.从before中获取
                            tableProcessDim = jsonObject.getObject("before", TableProcessDim.class);
                        } else {
                            //对配置表进行了读取,添加,修改,从after中获取
                            tableProcessDim = jsonObject.getObject("after", TableProcessDim.class);
                        }
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);
//        tpDs.print("配置表实体类");

        // TODO: 2024/12/12根据配置表中的配置信息到HBase中执行建表等操作

//        SingleOutputStreamOperator<TableProcessDim> createHbase = tpDs.map(
//                new RichMapFunction<TableProcessDim, TableProcessDim>() {
//
//                    private Connection hbaseConn;
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        hbaseConn = HBaseutill.getHBaseConnection();
//                    }
//
//                    @Override
//                    public void close() throws Exception {
//                        HBaseutill.closeHBaseConnection(hbaseConn);
//                    }
//
//                    @Override
//                    public TableProcessDim map(TableProcessDim tp) throws Exception {
//                        //获取对配置表进行的操作的类型
//                        String op = tp.getOp();
//                        //获取Hbase中维度表的表名
//                        String sinkTable = tp.getSinkTable();
//                        //获取在HBase中建表的列族
//                        String[] sinkFamilies = tp.getSinkFamily().split(",");
//                        if ("d".equals(op)) {
//                            //从配置表中删除了一条数据，将hbase中对应的表删除掉
//                            HBaseutill.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
//                        } else if ("r".equals(op) || "c".equals(op)) {
//                            //从配置表中读取了一条数据或者向配置表中了一条配置      在hbase中执行建表
//                            HBaseutill.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
//                        } else {
//                            //对配置表中的配置信息进行了修改     先从Hhbase中将对应的表删除掉，再创建新表
//                            HBaseutill.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
//                            HBaseutill.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
//
//                        }
//                        return tp;
//                    }
//                }
//        ).setParallelism(1);
//        createHbase.print();


        //TODO 8.将配置流中的配置信息进行广播---broadcast
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor
                = new MapStateDescriptor<String, TableProcessDim>("mapStateDescriptor",String.class,TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDs.broadcast(mapStateDescriptor);
        //TODO 9.将主流业务数据和广播流配置信息进行关联---connect
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);
        //TODO 10.处理关联后的数据（判断是否为维度）
        SingleOutputStreamOperator<Tuple2<JSONObject,TableProcessDim>> dimDS = connectDS.process(
                new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>() {

                    private Map<String,TableProcessDim> configMap = new HashMap<>();
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //将配置表中的配置信息预加载到程序configMap中
                        //注册驱动
                        Class.forName("com.mysql.cj.jdbc.Driver");
                        //建立连接
                        java.sql.Connection conn = DriverManager.getConnection(Constant.MYSQL_URL, Constant.MYSQL_USER_NAME, Constant.MYSQL_PASSWORD);
                        //获取数据库操作对象
                        String sql = "select * from gmall_config.table_process_dim";
                        PreparedStatement ps = conn.prepareStatement(sql);
                        //执行SQL语句
                        ResultSet rs = ps.executeQuery();
                        ResultSetMetaData metaData = rs.getMetaData();
                        //处理结果集
                        while (rs.next()){
                            //定义一个json对象， 用于接收遍历出来的数据
                            JSONObject jsonObj = new JSONObject();
                            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                                String columnName = metaData.getColumnName(i);
                                Object columnValue = rs.getObject(i);
                                jsonObj.put(columnName,columnValue);
                            }
                            //将jsonObj转换为实体类对象，并放到configMap中
                            TableProcessDim tableProcessDim = jsonObj.toJavaObject(TableProcessDim.class);
                            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
                        }
                        //释放资源
                        rs.close();
                        ps.close();
                        conn.close();
                    }

                    //处理主流业务数据      根据维度表名到广播状态中读取配置信息，判断是否为维度
                    @Override
                    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject,TableProcessDim>> out) throws Exception {
                        //获取处理的数据的表名
                        String table = jsonObj.getString("table");
                        //获取广播状态
                        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        //根据表名先到广播状态中获取对应的配置信息，如果没有找到对应的配置，在尝试到configMap中获取
                        TableProcessDim tableProcessDim = null;

                        if ((tableProcessDim = broadcastState.get(table)) != null
                                || (tableProcessDim = configMap.get(table))!= null){
                            //如果根据表名获取到了对应的配置信息，说明当前处理的是维度数据
                            // 将维度数据继续向下游传递（只需要传递data属性内容即可）
                            JSONObject dataJsonObj = jsonObj.getJSONObject("data");

                            //在向下游传递数据前，过滤掉不需要传递的属性
                            String sinkColumns = tableProcessDim.getSinkColumns();
                            deleteNotNeedColumns(dataJsonObj,sinkColumns);
                            //在向下游传递数据前，补充对维度数据的操作类型属性
                            String type = jsonObj.getString("type");
                            dataJsonObj.put("type",type);

                            out.collect(Tuple2.of(dataJsonObj,tableProcessDim));

                        }


                    }
                    //处理广播流配置信息     将配置数据放到广播状态中或者从广播状态中删除对应的配置   k:维度名    V:一个配置对象
                    @Override
                    public void processBroadcastElement(TableProcessDim tp, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>.Context cxt, Collector<Tuple2<JSONObject,TableProcessDim>> out) throws Exception {
                        //获取对配置表进行的操作的类型
                        String op = tp.getOp();
                        //获取广播状态
                        BroadcastState<String, TableProcessDim> broadcastState = cxt.getBroadcastState(mapStateDescriptor);
                        //获取维度表名称
                        String sourceTable = tp.getSourceTable();
                        if ("d".equals(op)){
                            //从配置表中删除了一条数据，将对应的配置信息也从广播状态中删除
                            broadcastState.remove(sourceTable);
                            configMap.remove(sourceTable);
                        }else {
                            //对配置表进行了读取、添加、或者更新操作，将最新的配置信息放到广播状态中
                            broadcastState.put(sourceTable,tp);
                            configMap.put(sourceTable,tp);
                        }
                    }
                }
        );





        env.execute();
    }

}
