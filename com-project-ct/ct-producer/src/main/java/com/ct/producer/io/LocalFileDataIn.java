package com.ct.producer.io;

import com.ct.commom.bean.Data;
import com.ct.commom.bean.Datain;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/*
本地文件输入
 */
public class LocalFileDataIn implements Datain {

    private BufferedReader reader = null;

    public LocalFileDataIn(String path) {
        setPath(path);
    }

    @Override
    public void setPath(String path) {

        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(path),"utf-8"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Object read() throws IOException {
        return null;
    }


    @Override
    public <T extends Data> List<T> read(Class<T> clazz) throws IOException {

        ArrayList<T> ts = new ArrayList<>();
        try {
            //将数据从文件中读取所有的数据
            String line =null;
            while ((line = reader.readLine())!=null){
                //将读取的数据转换为想要的格式
                T t = clazz.newInstance();
                t.setValue(line);
                ts.add(t);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ts;
    }

    @Override
    public void close() throws IOException {
        if (reader != null){
            reader.close();
        }
    }
}
