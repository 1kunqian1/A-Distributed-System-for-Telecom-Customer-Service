package com.ct.commom.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/*
日期工具类
 */
public class DateUtil {
    public static Date parse(String dataString, String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        Date date = null;
        try {
            date =sdf.parse(dataString);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date;
    }


    public static String format(Date date,String format){
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(date);
    }
}