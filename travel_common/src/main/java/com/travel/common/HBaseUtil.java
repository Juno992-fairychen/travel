package com.travel.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.*;

public class HBaseUtil {
    private static Connection connection = null;

    /**
     * 初始化hbase的连接
     */
    private static void initConnection() throws IOException {
        if (connection == null || connection.isClosed()) {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "hadoop2429:2181,hadoop2428:2181,hadoop2427:2181");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            connection = ConnectionFactory.createConnection(conf);
        }
    }

    /**
     * 获得连接
     */
    public static Connection getConnection() throws IOException {
        if (connection == null || connection.isClosed()) {
            initConnection();
        }
        return connection;
    }

    /**
     * 创建表（使用新 API）
     */
    public static void createTable(Connection connection, String tableNameString, String columnFamily) throws IOException {
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(tableNameString);

        if (!admin.tableExists(tableName)) {
            // 使用新 API 创建表
            TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(tableName);
            ColumnFamilyDescriptorBuilder cfBuilder = ColumnFamilyDescriptorBuilder.newBuilder(columnFamily.getBytes());
            ColumnFamilyDescriptor family = cfBuilder.build();
            tableBuilder.setColumnFamily(family);
            admin.createTable(tableBuilder.build());
        } else {
            // 检查列族是否存在，不存在则添加
            TableDescriptor tableDescriptor = admin.getDescriptor(tableName);
            boolean hasFamily = false;
            for (ColumnFamilyDescriptor familyDesc : tableDescriptor.getColumnFamilies()) {
                if (familyDesc.getNameAsString().equals(columnFamily)) {
                    hasFamily = true;
                    break;
                }
            }
            if (!hasFamily) {
                ColumnFamilyDescriptorBuilder cfBuilder = ColumnFamilyDescriptorBuilder.newBuilder(columnFamily.getBytes());
                admin.addColumnFamily(tableName, cfBuilder.build());
            }
        }
        admin.close();
    }

    /**
     * 判断hbase的表是否存在
     */
    public static boolean tableExists(String tableName) throws Exception {
        if (connection == null || connection.isClosed()) {
            initConnection();
        }
        Admin admin = connection.getAdmin();
        return admin.tableExists(TableName.valueOf(tableName));
    }

    public static Table getTable(String tableName) throws Exception {
        Connection connection = getConnection();
        return connection.getTable(TableName.valueOf(tableName));
    }

    public static void savePuts(List<Put> putList, String tableName) throws Exception {
        if (!tableExists(tableName)) {
            createTable(getConnection(), tableName, Constants.DEFAULT_FAMILY);
        }
        Table table = getTable(tableName);
        table.put(putList);
        table.close();
    }

    /**
     * 获取插入HBase的操作put
     */
    public static Put createPut(String rowKeyString, byte[] familyName, String columnName, String columnValue) {
        byte[] rowKey = rowKeyString.getBytes();
        Put put = new Put(rowKey);
        put.addColumn(familyName, columnName.getBytes(), columnValue.getBytes());
        return put;
    }

    /**
     * 获取插入HBase的操作put（批量列）
     */
    public static Put createPut(String rowKeyString, byte[] familyName, Map<String, String> columns) {
        byte[] rowKey = rowKeyString.getBytes();
        Put put = new Put(rowKey);
        for (Map.Entry<String, String> entry : columns.entrySet()) {
            put.addColumn(familyName, entry.getKey().getBytes(), entry.getValue().getBytes());
        }
        return put;
    }

    /**
     * 打印HBase查询结果
     */
    public static void print(Result result) {
        byte[] row = result.getRow();
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
        for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyEntry : map.entrySet()) {
            byte[] familyBytes = familyEntry.getKey();
            for (Map.Entry<byte[], NavigableMap<Long, byte[]>> entry : familyEntry.getValue().entrySet()) {
                byte[] column = entry.getKey();
                for (Map.Entry<Long, byte[]> longEntry : entry.getValue().entrySet()) {
                    Long time = longEntry.getKey();
                    byte[] value = longEntry.getValue();
                    System.out.println(String.format("行键rowKey=%s,列族columnFamily=%s,列column=%s,时间戳timestamp=%d,值value=%s",
                            new String(row), new String(familyBytes), new String(column), time, new String(value)));
                }
            }
        }
    }

    /**
     * 根据表名开始时间和结束时间查询轨迹
     */
    public static <T> List<T> getRest(String tableName, String orderId,
                                      String startTimestampe, String endTimestampe, Class<T> clazz) throws Exception {
        Table table = null;
        Scan scanner = null;
        List<T> restList = null;
        try {
            restList = new ArrayList<>();
            table = getTable(tableName);
            String startRowKey = orderId + "_" + startTimestampe;
            String endRowKey = orderId + "_" + endTimestampe;
            scanner = new Scan();
            scanner.setStartRow(startRowKey.getBytes());
            scanner.setStopRow(endRowKey.getBytes());

            try (ResultScanner rs = table.getScanner(scanner)) {
                for (Result r : rs) {
                    Field[] fields = clazz.getDeclaredFields();
                    T t = clazz.newInstance();
                    NavigableMap<byte[], byte[]> familyMap = r.getFamilyMap(Constants.DEFAULT_FAMILY.getBytes());
                    for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
                        String colName = Bytes.toString(entry.getKey());
                        String colValue = Bytes.toString(entry.getValue());

                        for (Field field : fields) {
                            String fieldName = field.getName();
                            field.setAccessible(true);
                            if (fieldName.equalsIgnoreCase(colName)) {
                                String fieldType = field.getType().toString();
                                if (fieldType.equalsIgnoreCase("class java.lang.String")) {
                                    field.set(t, colValue);
                                } else if (fieldType.equalsIgnoreCase("class java.lang.Integer")) {
                                    field.set(t, Integer.parseInt(colValue));
                                } else if (fieldType.equalsIgnoreCase("class java.lang.Long")) {
                                    field.set(t, Long.parseLong(colValue));
                                } else if (fieldType.equalsIgnoreCase("class java.lang.Double")) {
                                    field.set(t, Double.parseDouble(colValue));
                                } else if (fieldType.equalsIgnoreCase("class java.util.Date")) {
                                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                                    field.set(t, sdf.format(new Date(Long.parseLong(colValue + "000"))));
                                } else if (fieldType.equalsIgnoreCase("java.lang.Boolean")) {
                                    field.set(t, Boolean.parseBoolean(colValue));
                                } else {
                                    field.set(t, null);
                                }
                            }
                        }
                    }
                    restList.add(t);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return restList;
    }

    public static void main(String[] args) throws IOException {
        Connection connection = getConnection();
        Admin admin = connection.getAdmin();
        System.out.println("HBase connection successful!");
        admin.close();
    }
}