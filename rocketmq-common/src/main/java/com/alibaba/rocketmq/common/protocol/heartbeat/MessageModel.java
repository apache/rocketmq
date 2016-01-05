/**
 * $Id: MessageModel.java 1835 2013-05-16 02:00:50Z shijia.wxr $
 */
package com.alibaba.rocketmq.common.protocol.heartbeat;

/**
 * Message model
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public enum MessageModel {
    /**
     * broadcast
     */
    BROADCASTING("广播消费"),
    /**
     * clustering
     */
    CLUSTERING("集群消费");

    private String modeCN;

    MessageModel(String modeCN) {
        this.modeCN = modeCN;
    }


    public String getModeCN() {
        return modeCN;
    }
}
