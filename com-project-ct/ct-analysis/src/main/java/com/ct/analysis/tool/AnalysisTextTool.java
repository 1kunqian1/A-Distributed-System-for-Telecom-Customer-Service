package com.ct.analysis.tool;

import com.ct.analysis.io.MysqlTextOutputFormat;
import com.ct.analysis.mapper.AnalysisMapper;
import com.ct.analysis.reducer.AnalysisTextReducer;
import com.ct.commom.contant.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;
/*
分析数据的工具类
 */
public class AnalysisTextTool implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(AnalysisTextTool.class);

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(Names.CF_CALLER.getValue()));

        //mapper
        TableMapReduceUtil.initTableMapperJob(
                Names.TABLE.getValue(),
                scan,
                AnalysisMapper.class,
                Text.class,
                Text.class,
                job
        );
        //reducer
        job.setReducerClass(AnalysisTextReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //outputFormat
        job.setOutputFormatClass(MysqlTextOutputFormat.class);


        boolean flag = job.waitForCompletion(true);
        if (flag){
            return JobStatus.State.SUCCEEDED.getValue();
        }
        else
        {
            return JobStatus.State.FAILED.getValue();
        }
    }

    @Override
    public void setConf(Configuration configuration) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }
}
