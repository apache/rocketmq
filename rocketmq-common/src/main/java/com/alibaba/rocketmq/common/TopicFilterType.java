/**
 * $Id: TopicFilterType.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.common;

/**
 * Topic过滤方式，默认为单TAG过滤
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public enum TopicFilterType {
    /**
     * 每个消息只能有一个Tag
     */
    SINGLE_TAG,
    /**
     * 每个消息可以有多个Tag
     */
    MULTI_TAG
}
