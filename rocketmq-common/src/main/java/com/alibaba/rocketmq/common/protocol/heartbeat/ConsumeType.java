/**
 * $Id: ConsumeType.java 1835 2013-05-16 02:00:50Z shijia.wxr $
 */
package com.alibaba.rocketmq.common.protocol.heartbeat;

/**
 * 消费类型
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public enum ConsumeType {
    /**
     * 主动方式消费
     */
    CONSUME_ACTIVELY,
    /**
     * 被动方式消费
     */
    CONSUME_PASSIVELY,
}
