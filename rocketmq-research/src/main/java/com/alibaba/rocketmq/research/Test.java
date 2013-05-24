package com.alibaba.rocketmq.research;

/**
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public class Test {

    public static void main(String[] args) {
        byte[] abc = "abc".getBytes();

        Object obj = abc;
        System.out.println(obj);
    }
}
