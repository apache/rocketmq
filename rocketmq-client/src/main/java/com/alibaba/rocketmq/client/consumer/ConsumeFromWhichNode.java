/**
 * $Id: ConsumeFromWhichNode.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.consumer;

/**
 * Consumer从Master还是Slave消费消息
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * 
 */
public enum ConsumeFromWhichNode {
    /**
     * 优先从Master拉消息，如果Master不存在或者消息堆积，则转向Slave
     */
    CONSUME_FROM_MASTER_FIRST,
    /**
     * 优先从Slave拉消息，如果Slave不存在，则转向Master
     */
    CONSUME_FROM_SLAVE_FIRST,
    /**
     * 只从Master拉消息
     */
    CONSUME_FROM_MASTER_ONLY,
    /**
     * 只从Slave拉消息
     */
    CONSUME_FROM_SLAVE_ONLY,
}
