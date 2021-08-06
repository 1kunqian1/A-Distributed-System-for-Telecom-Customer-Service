package com.ct.commom.bean;

public abstract class Data implements Value {

    public String content;

    @Override
    public String getValue() {
        return content;
    }

    @Override
    public void setValue(Object val) {
        content = (String) val;
    }
}