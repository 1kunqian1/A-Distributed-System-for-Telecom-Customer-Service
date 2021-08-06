package com.ct.commom.util;

import java.text.DecimalFormat;

/*
数字的工具类
 */
public class NumberUtil {
    /*
   将数字格式化为字符串
     */
    public static String format(int num, int length) {
        StringBuffer stringBuffer = new StringBuffer();
        for (int i = 0; i < length; i++) {
            stringBuffer.append("0");
        }
        DecimalFormat df = new DecimalFormat(stringBuffer.toString());
        return df.format(num);
    }
}

