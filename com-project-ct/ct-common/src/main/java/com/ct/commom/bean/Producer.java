package com.ct.commom.bean;

import java.io.Closeable;

/*
    生产者接口
 */
public interface Producer extends Closeable {

    //数据产生的来源
    public void setin(Datain in);
    //数据的去向
    public void setout(Dataout out);
    //生产数据方法
    public void produce();
}