package com.ct.consumer.bean;

import com.ct.commom.api.TableRef;
import com.ct.commom.bean.Consumer;
import com.ct.commom.contant.Names;
import com.ct.consumer.dao.HBaseDao;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class CalllogConsumer implements Consumer {

    @Override
    public void consume() {

        //创建配置对象
        Properties properties = new Properties();
        try {
            properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("consumer.properties"));
            //  properties.load(Consumer.class.getClassLoader().getResourceAsStream("consumer.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        //获取Flume的数据
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        //订阅主题
        consumer.subscribe(Arrays.asList(Names.TOPIC.getValue()));
        //HBase数据访问对象
        HBaseDao hBaseDao = new HBaseDao();
        //初始化
        try {
            hBaseDao.init();
        } catch (Exception e) {
            e.printStackTrace();
        }
        //消费数据
        while (true) {
            ConsumerRecords<String, String> poll = consumer.poll(100);
            for (ConsumerRecord<String, String> stringStringConsumerRecord : poll) {
                //插入数据
                //   System.out.println(stringStringConsumerRecord.value());
                try {
                   hBaseDao.insertData(stringStringConsumerRecord.value());
                  //  CallLog callLog = new CallLog(stringStringConsumerRecord.value());
                //    hBaseDao.insertdata(callLog);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void close() throws IOException {

    }
}
