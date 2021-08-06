package com.ct.analysis.mapper;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/*
分析数据的mapper
 */
public class AnalysisMapper extends TableMapper<Text,Text> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        String rowkey = Bytes.toString(key.get());
        String[] s = rowkey.split("_");
        String call1 = s[1] ;
        String call2 = s[3];
        String calltime = s[2] ;
        String duration = s[4];

        String year = calltime.substring(0,4);
        String month = calltime.substring(0,6);
        String day = calltime.substring(0,8);


        //主叫用户-年
        context.write(new Text(call1 + "_" + year),new Text(duration));
        //主叫用户-月
        context.write(new Text(call1 + "_" + month),new Text(duration));
        //主叫用户-日
        context.write(new Text(call1 + "_" + day),new Text(duration));
        //被叫用户-年
        context.write(new Text(call2 + "_" + year),new Text(duration));
        //被叫用户-年
        context.write(new Text(call2 + "_" + month),new Text(duration));
        //被叫用户-年
        context.write(new Text(call2 + "_" + day),new Text(duration));
    }
}
