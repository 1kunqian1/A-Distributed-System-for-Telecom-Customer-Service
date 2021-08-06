package com.ct.consumer;

import com.ct.commom.bean.Consumer;
import com.ct.consumer.bean.CalllogConsumer;

import java.io.IOException;

/*
启动消费者
 */
public class Bootstrap {
    public static void main(String[] args) throws IOException {

        //创建消费者
        Consumer consumer = new CalllogConsumer();
        //消费数据
        consumer.consume();
        //关闭资源
        consumer.close();
    }
}
