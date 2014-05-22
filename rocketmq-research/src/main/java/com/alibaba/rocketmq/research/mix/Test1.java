/**
 * $Id: Test1.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.research.mix;

/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class Test1 {

    public static void test_1(String[] args) {
        long timeLong = System.currentTimeMillis();
        System.out.println(" timeLong = " + timeLong);
        int timeInt = (int) (timeLong / 1000);
        System.out.println(" timeInt = " + timeInt);
    }


    public static void test_2(String[] args) {
        long d1 = 10000;
        double d2 = 6;

        System.out.println(String.format("%.2f", d1 / d2));
    }


    public static void main(String[] args) {
        test_2(args);
    }
}
