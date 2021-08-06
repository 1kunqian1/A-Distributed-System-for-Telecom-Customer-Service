package com.ct.commom.bean;


import com.ct.commom.contant.Names;
import com.ct.commom.contant.ValueConstant;
import com.ct.commom.util.DateUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public abstract class BaseDao {

    private ThreadLocal<Connection> connHolder = new ThreadLocal<>();
    private ThreadLocal<Admin> adminHolder = new ThreadLocal<>();


    protected void start() throws IOException {
        getConnection();
        getAdmin();
    }

    protected void stop() throws Exception {
        Admin admin = getAdmin();
        if (admin != null) {
            admin.close();
            adminHolder.remove();
        }

        Connection connection = getConnection();
        if (connection == null) {
            connection.close();
            connHolder.remove();
        }
    }

    /*
    获取连接
     */
    protected synchronized Connection getConnection() throws IOException {
        Connection conn = connHolder.get();
        if (conn == null) {
            Configuration conf = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection(conf);
            connHolder.set(conn);
        }
        return conn;
    }

    /*
    获取管理者
     */

    protected synchronized Admin getAdmin() throws IOException {
        Admin admin = adminHolder.get();
        if (admin == null) {
            admin = getConnection().getAdmin();
            adminHolder.set(admin);
        }
        return admin;
    }

    /*
    创建命名空间（数据库）
     */
    protected void createNamespaceNX(String namespace) throws Exception {
        Admin admin = getAdmin();
        try {
            admin.getNamespaceDescriptor(namespace);
        } catch (NamespaceNotFoundException e) {
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(namespaceDescriptor);
        }
    }

    /*
    创建表
     */
    protected void createTableXX(String tableName,String... family) throws Exception {
        createTableXX(tableName, null,null, family);
    }

    protected void createTableXX(String tableName, String coprocessorClass,Integer regionCount, String... family) throws Exception {
        Admin admin = getAdmin();
        TableName tableName1 = TableName.valueOf(tableName);
        boolean b = admin.tableExists(tableName1);
        if (b) {
            //表存在，需要删除
            deleteTable(tableName);
        }
        //创建新表
        createTable(tableName, coprocessorClass,regionCount, family);
    }

    private void createTable(String name, String coprocessorClass,Integer regionCount, String... family) throws Exception {
        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(name);
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        if (family.length == 0 || family == null) {
            family = new String[1];
            family[0] = Names.CF_INFO.getValue();
        }
        for (String s : family) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(s);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        if (coprocessorClass != null && !"".equals(coprocessorClass)){
            hTableDescriptor.addCoprocessor(coprocessorClass);
        }


        //增加预分区
        if (regionCount == null || regionCount <= 1) {
            admin.createTable(hTableDescriptor);
        } else {
            //分区键
            byte[][] bytes = getSplitkeys(regionCount);
             admin.createTable(hTableDescriptor,bytes);
        }
    }
    protected void deleteTable(String name) throws Exception {
        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(name);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }

    /*
    生成分区键
     */
    private byte[][] getSplitkeys (Integer regioncount){
        byte[][] bytes = new byte[regioncount-1][];
        //0|，1|，2|，3|，4|
        //（-∞,0|),[0|,1|)
        ArrayList<byte[]> bsList = new ArrayList<>();
        for (int i = 0; i < (regioncount-1); i++) {
            String splitKey = i + "|";
            bsList.add(Bytes.toBytes(splitKey));
        }

        bsList.toArray(bytes);
        return bytes;
    }

    /*
    计算分区号
     */
    protected int genRegionNum(String tel ,String data){
        String usercode = tel.substring(tel.length()-4);
        String yearMonth = data.substring(0,6);

        int userCodeHash = usercode.hashCode();
        int yearMonthHash = yearMonth.hashCode();

        //采用crc校验异或算法
        int crc = Math.abs(userCodeHash ^ yearMonthHash);
        int regionNum = crc % ValueConstant.REGION_COUNT;
        return regionNum;

    }

    /*
    查询时，获取开始和结束的集合
     */

    protected  List<String[]> getStartStoreRowkeys(String tel,String start ,String end){
        ArrayList<String[]> rowkeys = new ArrayList<>();
        String startTime = start.substring(0,6);
        String endTime = end.substring(0,6);

        Calendar startCal = Calendar.getInstance();
        startCal.setTime(DateUtil.parse(startTime,"yyyyMM"));

        Calendar endCal = Calendar.getInstance();
        endCal.setTime(DateUtil.parse(endTime,"yyyyMM"));

        while (startCal.getTimeInMillis() <= endCal.getTimeInMillis()){

            String nowTime = DateUtil.format(startCal.getTime(), "yyyyMM");
            int regionNum = genRegionNum(tel,nowTime);
            String startRow = regionNum + "_" + tel + "_" + nowTime;
            String stopRow = startRow + "|";
            String[] rowKey = {startRow, stopRow};
            rowkeys.add(rowKey);
            startCal.add(Calendar.MONTH,1);
        }
        return  rowkeys;
    }

}
