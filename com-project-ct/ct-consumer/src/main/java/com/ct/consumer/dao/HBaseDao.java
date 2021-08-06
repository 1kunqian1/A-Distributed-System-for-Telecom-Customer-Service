package com.ct.consumer.dao;

import com.ct.commom.api.Column;
import com.ct.commom.api.Rowkey;
import com.ct.commom.api.TableRef;
import com.ct.commom.bean.BaseDao;
import com.ct.commom.contant.Names;
import com.ct.commom.contant.ValueConstant;
import com.ct.consumer.bean.CallLog;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.List;

public class HBaseDao extends BaseDao {

    /*
    初始化
     */
    public void init() throws Exception {
        start();

        createNamespaceNX(Names.NAMESPACE.getValue());
        createTableXX(Names.TABLE.getValue(),"com.ct.consumer.coprocessor.InsertCalleeCoprocessor", ValueConstant.REGION_COUNT, Names.CF_CALLER.getValue(),Names.CF_CALLEE.getValue());
        stop();
    }

    /*
    插入数据
     */
    public void insertData(String data) throws Exception {

        //将通话日志添加到HBase中
        //1获取通话日志
        //System.out.println(data);
        String[] split = data.split("\t");
        String call1 = split[0];
        String call2 = split[1];
        String callTime = split[2];
        String duration = split[3];

        //创建对象

        //主叫用户
        String rowKey = genRegionNum(call1, callTime) + "_" + call1 + "_" + callTime + "_" + call2 + "_" + duration + "_1";
        Put put = new Put(Bytes.toBytes(rowKey));
        byte[] family = Bytes.toBytes(Names.CF_CALLER.getValue());
        put.addColumn(family, Bytes.toBytes("call1"), Bytes.toBytes(call1));
        put.addColumn(family, Bytes.toBytes("call2"), Bytes.toBytes(call2));
        put.addColumn(family, Bytes.toBytes("callTime"), Bytes.toBytes(callTime));
        put.addColumn(family, Bytes.toBytes("duration"), Bytes.toBytes(duration));

        //被叫用户
//        String calleeRowKey = genRegionNum(call2, callTime) + "_" + call2 + "_" + callTime + "_" + call1 + "_" + duration + "_0";
//        Put calleePut = new Put(Bytes.toBytes(calleeRowKey));
//        byte[] calleeFamily = Bytes.toBytes(Names.CF_CALLEE.getValue());
//
//        calleePut.addColumn(calleeFamily, Bytes.toBytes("call1"), Bytes.toBytes(call2));
//        calleePut.addColumn(calleeFamily, Bytes.toBytes("call2"), Bytes.toBytes(call1));
//        calleePut.addColumn(calleeFamily, Bytes.toBytes("callTime"), Bytes.toBytes(callTime));
//        calleePut.addColumn(calleeFamily, Bytes.toBytes("duration"), Bytes.toBytes(duration));

        //保存数据
        putData(Names.TABLE.getValue(), put);
     //   putData(Names.TABLE.getValue(), calleePut);

    }

    /*
    一次插入一条数据
     */
    protected void putData(String name, Put put) throws IOException {
        //获取表对象
        Connection conn = getConnection();
        Table table = conn.getTable(TableName.valueOf(name));
        //插入数据
        table.put(put);
        //关闭表
        table.close();
    }
    /*
    一次插入多条数据
     */
    protected void putData(String name, List<Put> puts) throws IOException {
        //获取表对象
        Connection conn = getConnection();
        Table table = conn.getTable(TableName.valueOf(name));
        //插入数据
        table.put(puts);
        //关闭表
        table.close();
    }

    /*
    插入对象
     */

    public void insertdata(CallLog log) throws Exception {
        log.setRowKeys(genRegionNum(log.getCall1(), log.getCallTime()) + "_" + log.getCall1() + "_" + log.getCallTime() + "_" + log.getCall2() + "_" + log.getCallTime());
        putData(log);
    }

    //增加对象，直接封装到HBase
    private void putData(Object log) throws Exception {
        Class<?> aClass = log.getClass();
        TableRef annotation = aClass.getAnnotation(TableRef.class);
        Field[] fields = aClass.getDeclaredFields();
        String rowKeys = null;
        for (Field field : fields) {
            Rowkey annotation1 = field.getAnnotation(Rowkey.class);
            if (annotation1 != null) {
                field.setAccessible(true);
                rowKeys = (String) field.get(log);
                break;
            }
        }
        Put put = new Put(Bytes.toBytes(rowKeys));

        for (Field field : fields) {
            Column column = field.getAnnotation(Column.class);
            if (column != null) {
                String family = column.family();
                String colunm = column.colunm();
                if (colunm == null || "".equals(colunm)){
                    colunm = field.getName();
                }
                field.setAccessible(true);
                String value = (String) field.get(log);
                put.addColumn(Bytes.toBytes(family),Bytes.toBytes(colunm),Bytes.toBytes(value));
            }
        }
        putData(annotation.value(),put);
    }

}
