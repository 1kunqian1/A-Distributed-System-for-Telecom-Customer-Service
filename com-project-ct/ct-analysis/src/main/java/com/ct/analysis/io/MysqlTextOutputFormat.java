package com.ct.analysis.io;

import com.ct.commom.util.JDBCUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/*
MYSQL的输出对象的格式化
 */
public class MysqlTextOutputFormat extends OutputFormat {


    protected static class MySQLRecordWriter extends RecordWriter<Text, Text> {
        public Connection conn = null;
        public MySQLRecordWriter() {
            conn = JDBCUtil.getConnection();
        }

        /*
        输出数据
         */
        @Override
        public void write(Text text, Text text2) throws IOException, InterruptedException {
            PreparedStatement statement = null;

            String[] s = text2.toString().split("_");
            String sumcall = s[0];
            String sumduration = s[1];
            try {
                String insertSQL = "insert int ct_call(telid,dateid,suncall,callduration)values(?,?,?,?)";
                statement = conn.prepareStatement(insertSQL);
                statement.setInt(1,1);
                statement.setInt(2,2);
                statement.setInt(3,Integer.parseInt(sumcall));
                statement.setInt(4,Integer.parseInt(sumduration));
                statement.executeUpdate();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }finally {
                if (statement != null){
                    try {
                        statement.close();
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }
            }
        }

        /*
        释放资源
         */
        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
    }

    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new MySQLRecordWriter();

    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    private FileOutputCommitter committer = null;

    public static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null : new Path(name);
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        if (committer == null) {
            Path output = getOutputPath(taskAttemptContext);
            committer = new FileOutputCommitter(output, taskAttemptContext);
        }
        return committer;
    }
}
