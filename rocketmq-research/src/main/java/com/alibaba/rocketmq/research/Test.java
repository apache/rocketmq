package com.alibaba.rocketmq.research;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import com.alibaba.rocketmq.common.UtilAll;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class Test {

    public static long computNextMorningTimeMillis() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, 2013);
        cal.set(Calendar.MONTH, 10);
        cal.set(Calendar.DAY_OF_MONTH, 11);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }


    public static void main(String[] args) {
        long timestamp = System.currentTimeMillis() - (1000 * 60 * 20);
        String date = UtilAll.timeMillisToHumanString3(timestamp);
        System.out.println(date);

        long timestatmpNew = UtilAll.parseDate(date, UtilAll.yyyyMMddHHmmss).getTime();

        System.out.println(timestamp);
        System.out.println(timestatmpNew);
    }
}
