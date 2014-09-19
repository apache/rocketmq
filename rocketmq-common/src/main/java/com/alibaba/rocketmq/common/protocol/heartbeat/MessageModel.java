/**
 * $Id: MessageModel.java 1835 2013-05-16 02:00:50Z shijia.wxr $
 */
package com.alibaba.rocketmq.common.protocol.heartbeat;

/**
 * 消息模型
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public enum MessageModel {
    /**
     * 广播模型
     */
    BROADCASTING,
    /**
     * 集群模型
     */
    CLUSTERING,
    // /**
    // * 未知，如果是主动消费，很难确定应用的消息模型
    // */
    // UNKNOWNS,
}
