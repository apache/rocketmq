/**
 * $Id: FindBrokerResult.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.impl;

/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class FindBrokerResult {
    private final String brokerAddr;
    private final boolean slave;


    public FindBrokerResult(String brokerAddr, boolean slave) {
        this.brokerAddr = brokerAddr;
        this.slave = slave;
    }


    public String getBrokerAddr() {
        return brokerAddr;
    }


    public boolean isSlave() {
        return slave;
    }
}
