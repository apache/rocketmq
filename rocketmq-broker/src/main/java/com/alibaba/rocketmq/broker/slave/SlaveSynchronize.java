package com.alibaba.rocketmq.broker.slave;

import com.alibaba.rocketmq.broker.BrokerController;


/**
 * Slave从Master同步信息（非消息）
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-8
 */
public class SlaveSynchronize {
    private final BrokerController brokerController;
    private volatile String masterAddr = null;


    public SlaveSynchronize(BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    public String getMasterAddr() {
        return masterAddr;
    }


    public void setMasterAddr(String masterAddr) {
        this.masterAddr = masterAddr;
    }


    public void syncAll() {
        this.syncTopicConfig();
        this.syncConsumerOffset();
        this.syncDelayOffset();
    }


    private void syncTopicConfig() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null) {

        }
    }


    private void syncConsumerOffset() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null) {

        }
    }


    private void syncDelayOffset() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null) {

        }
    }
}
