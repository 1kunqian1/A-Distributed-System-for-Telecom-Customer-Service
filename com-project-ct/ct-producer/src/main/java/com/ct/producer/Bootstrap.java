package com.ct.producer;

import com.ct.producer.bean.LocalFileProducer;
import com.ct.producer.io.LocalFileDataIn;
import com.ct.producer.io.LocalFileDataOut;

import java.io.IOException;

public class Bootstrap {

    public static void main(String[] args) throws IOException {

        if (args.length <2){
            System.out.println("参数不对！给我重新输！");
            System.exit(1);
        }
        //创建生产者
        LocalFileProducer localFileProducer = new LocalFileProducer();
        localFileProducer.setin(new LocalFileDataIn(args[0]));
        localFileProducer.setout(new LocalFileDataOut(args[1]));
        //生产数据
        localFileProducer.produce();

        //关闭资源
        localFileProducer.close();
    }
}
