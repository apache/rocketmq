/**
 * $Id: PullRequest.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.impl.consumer;

/**
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * 
 */
public class PullRequest {
    private String consumerGroup;
    private String topic;
    private int queueId;
    private long nextOffset;
}
