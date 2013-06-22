package com.alibaba.rocketmq.client.impl.consumer;

import java.util.List;

import com.alibaba.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import com.alibaba.rocketmq.client.consumer.store.OffsetStore;
import com.alibaba.rocketmq.client.impl.factory.MQClientFactory;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-6-22
 */
public class RebalancePushImpl extends RebalanceImpl {
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;


    public RebalancePushImpl(String consumerGroup, MessageModel messageModel,
            AllocateMessageQueueStrategy allocateMessageQueueStrategy, MQClientFactory mQClientFactory,
            DefaultMQPushConsumerImpl defaultMQPushConsumerImpl) {
        super(consumerGroup, messageModel, allocateMessageQueueStrategy, mQClientFactory);
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
    }


    @Override
    public void dispatchPullRequest(List<PullRequest> pullRequestList) {
        // ÅÉ·¢PullRequest
        for (PullRequest pullRequest : pullRequestList) {
            this.defaultMQPushConsumerImpl.executePullRequestImmediately(pullRequest);
            log.info("doRebalance, {}, add a new pull request {}", consumerGroup);
        }
    }


    @Override
    public long computePullFromWhere(MessageQueue mq) {
        long result = -1;
        final ConsumeFromWhere consumeFromWhere =
                this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getConsumeFromWhere();
        final OffsetStore offsetStore = this.defaultMQPushConsumerImpl.getOffsetStore();
        switch (consumeFromWhere) {
        case CONSUME_FROM_LAST_OFFSET: {
            long lastOffset = offsetStore.readOffset(mq, true);
            if (lastOffset >= 0) {
                result = lastOffset;
            }
            else {
                result = Long.MAX_VALUE;
            }
            break;
        }
        case CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST: {
            long lastOffset = offsetStore.readOffset(mq, true);
            if (lastOffset >= 0) {
                result = lastOffset;
            }
            else {
                result = 0L;
            }
            break;
        }
        case CONSUME_FROM_MAX_OFFSET:
            result = Long.MAX_VALUE;
            break;
        case CONSUME_FROM_MIN_OFFSET:
            result = 0L;
            break;
        default:
            break;
        }

        return result;
    }
}
