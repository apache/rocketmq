/**
 * $Id: ConsumeConcurrentlyStatus.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.consumer.listener;

/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public enum ConsumeConcurrentlyStatus {
    CONSUME_SUCCESS,
    RECONSUME_LATER,
}
