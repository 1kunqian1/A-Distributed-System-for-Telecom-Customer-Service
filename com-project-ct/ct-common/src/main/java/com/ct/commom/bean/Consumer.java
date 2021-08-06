package com.ct.commom.bean;

import java.io.Closeable;

public interface Consumer extends Closeable{
    //消费数据方法
    public void consume();
}
