package com.alibaba.rocketmq.client.impl.producer;

import java.util.Set;

import com.alibaba.rocketmq.client.producer.TransactionCheckListener;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;


/**
 * Producer内部接口
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public interface MQProducerInner {
    public Set<String> getPublishTopicList();


    public TransactionCheckListener checkListener();


    public void checkTransactionState(//
            final String addr, //
            final MessageExt msg, //
            final CheckTransactionStateRequestHeader checkRequestHeader);


    public void updateTopicPublishInfo(final String topic, final TopicPublishInfo info);
}
