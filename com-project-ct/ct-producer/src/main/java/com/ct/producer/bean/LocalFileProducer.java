package com.ct.producer.bean;

import com.ct.commom.bean.Datain;
import com.ct.commom.bean.Dataout;
import com.ct.commom.bean.Producer;
import com.ct.commom.util.DateUtil;
import com.ct.commom.util.NumberUtil;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class LocalFileProducer implements Producer {
    private Datain in;
    private Dataout out;
    private volatile boolean flag = true;

    @Override
    public void setin(Datain in) {
        this.in = in;
    }

    @Override
    public void setout(Dataout out) {
        this.out = out;
    }

    @Override
    public void produce() {
        //读取通讯录的数据
        try {
            List<Contact> contacts = in.read(Contact.class);
            while (flag) {
                //  从通讯录中随机查找两个号码（主叫、被叫）
                int callindex1 = new Random().nextInt(contacts.size());
                int callindex2;
                while (true) {
                    callindex2 = new Random().nextInt(contacts.size());
                    if (callindex1 != callindex2) {
                        break;
                    }
                }
                Contact call1 = contacts.get(callindex1);
                Contact call2 = contacts.get(callindex2);

                // 生成随机的通话时间
                String startDate = "20180101000000";
                String endDate = "20190101000000";
                long startTime = DateUtil.parse(startDate, "yyyyMMddHHmmss").getTime();
                long endtTime = DateUtil.parse(endDate, "yyyyMMddHHmmss").getTime();

                long callTime = startTime + (long)((endtTime-startTime)*Math.random());
                String callDate = DateUtil.format(new Date(callTime),"yyyyMMddHHmmss");
                // 生成随机的通话时长
                String duration = NumberUtil.format(new Random().nextInt(3000),4);
                // 生成通话记录
                CallLog log = new CallLog(call1.getTel(),call2.getTel(),callDate,duration);
                // 将通话记录刷写到文件中
                System.out.println(log);
                try {
                    out.write(log);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    /*
    关闭生产者
     */
    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
        if (out != null) {
            out.close();
        }


    }
}