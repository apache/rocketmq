package com.alibaba.rocketmq.research;

import java.util.Calendar;

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


    private static String diskUtil() {
        String storePathPhysic = System.getenv("HOME");
        double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);

        String storePathLogis = storePathPhysic;
        double logisRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogis);

        String storePathIndex = storePathPhysic;
        double indexRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathIndex);

        return String.format("CL: %5.2f CQ: %5.2f INDEX: %5.2f", physicRatio, logisRatio, indexRatio);
    }


    public static void main(String[] args) {
        int a = -25;
        int b = 1;

        int c = a % b;
        System.out.println(c);

        System.out.println(diskUtil());
    }
}
