package com.ct.commom.contant;

import com.ct.commom.bean.Value;

public enum Names implements Value {
    NAMESPACE("ct"),
    TOPIC("ct"),
    CF_CALLER("caller"),
    CF_CALLEE("callee"),
    CF_INFO("info"),
    TABLE("ct:calllog");

    private String name;

    private Names(String name){
        this.name = name;
    }

    @Override
    public String getValue() {
        return name;
    }


    @Override
    public void setValue(Object val) {

    }
}
