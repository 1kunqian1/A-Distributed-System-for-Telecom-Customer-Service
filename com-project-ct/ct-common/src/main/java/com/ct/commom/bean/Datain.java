package com.ct.commom.bean;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface Datain extends Closeable {

    public void setPath(String path);

    public Object read() throws IOException;

    public <T extends Data> List<T> read(Class<T> clazz ) throws IOException;
}
