package com.alibaba.rocketmq.research;

import java.util.Calendar;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class Test {
    public static long computNextMorningTimeMillis() {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(System.currentTimeMillis());
        cal.add(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        System.out.println(cal.getTime().toLocaleString());

        return cal.getTimeInMillis();
    }


    public static void main(String[] args) {
        System.out.println(System.currentTimeMillis());
        System.out.println(computNextMorningTimeMillis());
    }
}
