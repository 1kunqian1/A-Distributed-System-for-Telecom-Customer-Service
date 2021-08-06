package com.ct.consumer.coprocessor;

import com.ct.commom.bean.BaseDao;
import com.ct.commom.contant.Names;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;

/*
使用协处理器插入被叫用户的信息
协处理器的使用
1.创建累
2.让表和协处理器产生关联
3.打依赖的包放到HBase中
 */
public class InsertCalleeCoprocessor extends BaseRegionObserver {

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        Table table = e.getEnvironment().getTable(TableName.valueOf(Names.TABLE.getValue()));

        //String calleeRowKey = genRegionNum(call2, callTime) + "_" + call2 + "_" + callTime + "_" + call1 + "_" + duration + "_0";
        CoprocessorDao dao = new CoprocessorDao();
        String rowKey = Bytes.toString(put.getRow());
        String[] s = rowKey.split("_");
        String call1 = s[1];
        String call2 = s[3];
        String callTime = s[2];
        String duration = s[4];
        String flag = s[5];

        if ("1".equals(flag)) {
            String calleeRowKey = dao.getRegionNum(call2, callTime) + "_" + call2 + "_" + callTime + "_" + call1 + "_" + duration + "_0";
            Put calleePut = new Put(Bytes.toBytes(calleeRowKey));
            byte[] calleeFamily = Bytes.toBytes(Names.CF_CALLEE.getValue());
            calleePut.addColumn(calleeFamily, Bytes.toBytes("call1"), Bytes.toBytes(call2));
            calleePut.addColumn(calleeFamily, Bytes.toBytes("call2"), Bytes.toBytes(call1));
            calleePut.addColumn(calleeFamily, Bytes.toBytes("callTime"), Bytes.toBytes(callTime));
            calleePut.addColumn(calleeFamily, Bytes.toBytes("duration"), Bytes.toBytes(duration));

            table.put(calleePut);
            table.close();
        }

    }

    private class CoprocessorDao extends BaseDao {
        public int getRegionNum(String tel, String data) {
            return genRegionNum(tel, data);
        }
    }
}
