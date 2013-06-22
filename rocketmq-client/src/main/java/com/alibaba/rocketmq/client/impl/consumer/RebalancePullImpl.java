package com.alibaba.rocketmq.client.impl.consumer;

import java.util.List;

import com.alibaba.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import com.alibaba.rocketmq.client.impl.factory.MQClientFactory;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-6-22
 */
public class RebalancePullImpl extends RebalanceImpl {

    public RebalancePullImpl(String consumerGroup, MessageModel messageModel,
            AllocateMessageQueueStrategy allocateMessageQueueStrategy, MQClientFactory mQClientFactory) {
        super(consumerGroup, messageModel, allocateMessageQueueStrategy, mQClientFactory);
    }


    @Override
    public void dispatchPullRequest(List<PullRequest> pullRequestList) {
    }


    @Override
    public long computePullFromWhere(MessageQueue mq) {
        return 0;
    }
}
