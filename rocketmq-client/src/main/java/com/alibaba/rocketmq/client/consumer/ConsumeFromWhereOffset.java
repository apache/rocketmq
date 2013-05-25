/**
 * $Id: ConsumeFromWhereOffset.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.consumer;

/**
 * Consumer从哪里开始消费
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public enum ConsumeFromWhereOffset {
    /**
     * 每次启动都从上次记录的位点开始消费，如果是第一次启动则从最大位点开始消费，建议在生产环境使用
     */
    CONSUME_FROM_LAST_OFFSET,
    /**
     * 每次启动都从上次记录的位点开始消费，如果是第一次启动则从最小位点开始消费，建议测试时使用
     */
    CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST,
    /**
     * 每次启动都从最小位点开始消费，建议测试时使用
     */
    CONSUME_FROM_MIN_OFFSET,
    /**
     * 每次启动都从最大位点开始消费，建议测试时使用
     */
    CONSUME_FROM_MAX_OFFSET,
}
