package com.alibaba.rocketmq.common.admin;

/**
 * Offset°ü×°Àà£¬º¬Broker¡¢Consumer
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-14
 */
public class OffsetWrapper {
    private long brokerOffset;
    private long consumerOffset;


    public long getBrokerOffset() {
        return brokerOffset;
    }


    public void setBrokerOffset(long brokerOffset) {
        this.brokerOffset = brokerOffset;
    }


    public long getConsumerOffset() {
        return consumerOffset;
    }


    public void setConsumerOffset(long consumerOffset) {
        this.consumerOffset = consumerOffset;
    }
}
