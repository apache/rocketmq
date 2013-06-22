package com.alibaba.rocketmq.client.impl.consumer;

import java.util.List;
import java.util.Set;

import com.alibaba.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import com.alibaba.rocketmq.client.consumer.MessageQueueListener;
import com.alibaba.rocketmq.client.impl.factory.MQClientFactory;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-6-22
 */
public class RebalancePullImpl extends RebalanceImpl {
    private final DefaultMQPullConsumerImpl defaultMQPullConsumerImpl;


    public RebalancePullImpl(DefaultMQPullConsumerImpl defaultMQPullConsumerImpl) {
        this(null, null, null, null, defaultMQPullConsumerImpl);
    }


    public RebalancePullImpl(String consumerGroup, MessageModel messageModel,
            AllocateMessageQueueStrategy allocateMessageQueueStrategy, MQClientFactory mQClientFactory,
            DefaultMQPullConsumerImpl defaultMQPullConsumerImpl) {
        super(consumerGroup, messageModel, allocateMessageQueueStrategy, mQClientFactory);
        this.defaultMQPullConsumerImpl = defaultMQPullConsumerImpl;
    }


    @Override
    public long computePullFromWhere(MessageQueue mq) {
        return 0;
    }


    @Override
    public void dispatchPullRequest(List<PullRequest> pullRequestList) {
    }


    @Override
    public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
        MessageQueueListener messageQueueListener =
                this.defaultMQPullConsumerImpl.getDefaultMQPullConsumer().getMessageQueueListener();
        if (messageQueueListener != null) {
            try {
                messageQueueListener.messageQueueChanged(topic, mqAll, mqDivided);
            }
            catch (Throwable e) {
                log.error("messageQueueChanged exception", e);
            }
        }
    }
}
