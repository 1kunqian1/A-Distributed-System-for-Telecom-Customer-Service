package com.ct.consumer.bean;

import com.ct.commom.api.Column;
import com.ct.commom.api.Rowkey;
import com.ct.commom.api.TableRef;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.ElementType.LOCAL_VARIABLE;


@TableRef("ct:calllog")
public class CallLog {
    @Rowkey
    private String rowKeys;
    @Column(family = "caller")
    private  String call1;
    @Column(family = "caller")
    private  String call2;
    @Column(family = "caller")
    private  String callTime;
    @Column(family = "caller")
    private  String duration;
    @Column(family = "caller")
    private String flag = "1";
    private String name;

    public CallLog() {
    }

    public CallLog(String data) {
        String[] split = data.split("\t");

        this.call1 = split[0];
        this.call2 = split[1];
        this.callTime = split[2];
        this.duration = split[3];
    }

    public String getCall1() {
        return call1;
    }

    public void setCall1(String call1) {
        this.call1 = call1;
    }

    public String getCall2() {
        return call2;
    }

    public void setCall2(String call2) {
        this.call2 = call2;
    }

    public String getCallTime() {
        return callTime;
    }

    public void setCallTime(String callTime) {
        this.callTime = callTime;
    }

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }

    public String getRowKeys() {
        return rowKeys;
    }

    public void setRowKeys(String rowKeys) {
        this.rowKeys = rowKeys;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
