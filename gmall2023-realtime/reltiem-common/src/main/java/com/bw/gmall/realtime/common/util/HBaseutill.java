package com.bw.gmall.realtime.common.util;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;


import org.apache.hadoop.hbase.client.ConnectionFactory;



public class HBaseutill {

    //获取Hbase连接
    public static Connection getHBaseConnection() throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
        Connection hbaseConn = ConnectionFactory.createConnection(conf);
        return hbaseConn;
    }
    //关闭Hbase连接
    public static void closeHBaseConnection(Connection hbaseConn) throws IOException {
        if (hbaseConn != null && !hbaseConn.isClosed()){
            hbaseConn.close();
        }
    }
    //建表
    public static void createHBaseTable(Connection hbaseConn,String namespace,String tableName,String ... families){
        if (families.length < 1){
            System.out.println("至少需要一个列族");
            return;
        }

        try (Admin admin = hbaseConn.getAdmin()){
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            if (admin.tableExists(tableNameObj)){
                System.out.println("表空间"+ namespace +"下的表"+ tableName +"已存在");
                return;
            }
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);
            for (String family : families) {
                ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build();
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
            }

            admin.createTable(tableDescriptorBuilder.build());

            System.out.println("表空间"+ namespace +"下的表"+ tableName +"创建成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    //删除表
    public static void dropHBaseTable(Connection connection,
                                      String nameSpace,
                                      String tableName)
    {
        try (Admin admin = connection.getAdmin()){
            TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
            //判断删除的表是否存在
            if(!admin.tableExists(tableNameObj)){
                System.out.println("要删除的"+nameSpace+"下的表"+tableName+"不存在");
                return;
            }
            admin.disableTable(tableNameObj);
            admin.deleteTable(tableNameObj);
            System.out.println("删除"+nameSpace+"下的表"+tableName+"成功了");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }


    //向表中put数据
    public static void putRow(Connection hbaseConn, String namespace, String tableName, String rowKey, String family, JSONObject jsonObj){
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)){
            Put put = new Put(Bytes.toBytes(rowKey));
            Set<String> columns = jsonObj.keySet();
            for (String column : columns) {
                String value = jsonObj.getString(column);
                if (StringUtils.isNoneEmpty(value)){
                    put.addColumn(Bytes.toBytes(family),Bytes.toBytes(column),Bytes.toBytes(value));
                }
            }
            table.put(put);
            System.out.println("向表空间"+ namespace +"下的表"+ tableName +"中put数据成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    //从表中删除数据
    public static void delRow(Connection hbaseConn, String namespace, String tableName, String rowKey){
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)){
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            System.out.println("向表空间"+ namespace +"下的表"+ tableName +"中删除数据"+ rowKey  +"成功");
        } catch (IOException e){
            throw new RuntimeException(e);
        }
    }












}
