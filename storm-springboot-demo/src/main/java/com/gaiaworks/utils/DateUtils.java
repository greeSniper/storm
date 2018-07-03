package com.gaiaworks.utils;

import org.apache.commons.lang3.time.FastDateFormat;

import java.text.ParseException;

/**
 * Created by 唐哲
 * 2018-02-18 20:10
 * 时间解析工具类
 */
public class DateUtils {

    private DateUtils() {}

    private static DateUtils instance;

    public static DateUtils getInstance() {
        if(instance == null) {
            instance = new DateUtils();
        }
        return instance;
    }

    private FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy.MM.dd HH:mm:ss");

    /**
     * 将字符串2018.02.14 11:40:15转换为时间戳
     */
    public long getTime(String time) throws ParseException {
        return dateFormat.parse(time).getTime();
    }

    /**
     * 将时间戳转换为字符串
     */
    public String getString(long timestamp) {
        return dateFormat.format(timestamp);
    }

    public static void main(String[] args) throws ParseException {
//        Long timestamp = DateUtils.getInstance().getTime("2018.02.14 11:40:15");
//        String time = DateUtils.getInstance().dateFormat.format(timestamp);
//        System.out.println(time);

        System.out.println(DateUtils.getInstance().getString(1519268524317L));
    }

}
