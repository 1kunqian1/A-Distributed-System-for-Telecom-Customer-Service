package com.ct.commom.bean;

import java.io.Closeable;

public interface Dataout extends Closeable {
    public void setPath(String path);

    public void write(Object date) throws Exception;
    public void write(String date) throws Exception;
}