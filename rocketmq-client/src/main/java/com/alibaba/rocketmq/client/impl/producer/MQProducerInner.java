package com.alibaba.rocketmq.client.impl.producer;

import java.util.Set;

import com.alibaba.rocketmq.client.producer.TransactionCheckListener;


/**
 * Producer内部接口
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public interface MQProducerInner {
    public Set<String> getPublishTopicList();


    public TransactionCheckListener checkListener();


    public void updateTopicPublishInfo(final String topic, final TopicPublishInfo info);
}
