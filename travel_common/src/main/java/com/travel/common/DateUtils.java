package com.travel.common;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {

    /**
     * 线程安全的 SimpleDateFormat 获取方式
     */
    private static SimpleDateFormat getDateFormat(String pattern) {
        return new SimpleDateFormat(pattern);
    }

    public static String formateDate(Date date, String formatPattern) {
        return getDateFormat(formatPattern).format(date);
    }

    public static String formateDate(String seconds, String formatPattern) {
        return getDateFormat(formatPattern).format(Long.parseLong(seconds) * 1000);
    }

    /**
     * 获取小时（0-23）
     */
    public static int getHour(String timestamp) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(Long.parseLong(timestamp) * 1000);
        return calendar.get(Calendar.HOUR_OF_DAY);
    }

    public static int getHour(long timestampMillis) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timestampMillis);
        return calendar.get(Calendar.HOUR_OF_DAY);
    }

    public static int getHour(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.HOUR_OF_DAY);
    }
}