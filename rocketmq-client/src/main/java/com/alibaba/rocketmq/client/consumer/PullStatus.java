/**
 * $Id: PullStatus.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.consumer;

/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public enum PullStatus {
    /**
     * 找到消息
     */
    FOUND,
    /**
     * 没有新的消息可以被拉取
     */
    NO_NEW_MSG,
    /**
     * 经过过滤后，没有匹配的消息
     */
    NO_MATCHED_MSG,
    /**
     * Offset不合法，可能过大或者过小
     */
    OFFSET_ILLEGAL
}
