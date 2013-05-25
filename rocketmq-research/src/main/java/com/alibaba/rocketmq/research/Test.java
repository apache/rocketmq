package com.alibaba.rocketmq.research;

/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class Test {

    public static void main(String[] args) {
        byte[] abc = "abc".getBytes();

        Object obj = abc;
        System.out.println(obj);
    }
}
