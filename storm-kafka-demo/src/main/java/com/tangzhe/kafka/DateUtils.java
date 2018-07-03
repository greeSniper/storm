package com.tangzhe.kafka;

import org.apache.commons.lang3.time.FastDateFormat;

import java.text.ParseException;

/**
 * Created by 唐哲
 * 2018-02-12 13:10
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

    private FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

    /**
     * 将字符串[2018-02-12 11:40:15]转换为时间戳
     */
    public long getTime(String time) throws ParseException {
        return dateFormat.parse(time.substring(1, time.length()-1)).getTime();
    }

}
