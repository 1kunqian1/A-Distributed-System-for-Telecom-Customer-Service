package com.ct.producer.io;

import com.ct.commom.bean.Dataout;

import java.io.*;

/*
本地文件输出
 */
public class LocalFileDataOut implements Dataout {

    private PrintWriter writer =null;
    public LocalFileDataOut(String path){
        setPath(path);
    }


    @Override
    public void setPath(String path) {
        try {
            writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path),"utf-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Object date) throws Exception {
        write(date.toString());
    }

    @Override
    public void write(String date) throws Exception {
        writer.println(date);
        writer.flush();
    }


    @Override
    public void close() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }


}
