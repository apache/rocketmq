/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.client.impl.consumer;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.alibaba.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import com.alibaba.rocketmq.client.consumer.store.OffsetStore;
import com.alibaba.rocketmq.client.consumer.store.ReadOffsetType;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.factory.MQClientInstance;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.ConsumeType;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-6-22
 */
public class RebalancePushImpl extends RebalanceImpl {
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;


    public RebalancePushImpl(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl) {
        this(null, null, null, null, defaultMQPushConsumerImpl);
    }


    public RebalancePushImpl(String consumerGroup, MessageModel messageModel,
            AllocateMessageQueueStrategy allocateMessageQueueStrategy, MQClientInstance mQClientFactory,
            DefaultMQPushConsumerImpl defaultMQPushConsumerImpl) {
        super(consumerGroup, messageModel, allocateMessageQueueStrategy, mQClientFactory);
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
    }


    @Override
    public void dispatchPullRequest(List<PullRequest> pullRequestList) {
        for (PullRequest pullRequest : pullRequestList) {
            this.defaultMQPushConsumerImpl.executePullRequestImmediately(pullRequest);
            log.info("doRebalance, {}, add a new pull request {}", consumerGroup, pullRequest);
        }
    }


    @Override
    public long computePullFromWhere(MessageQueue mq) {
        long result = -1;
        final ConsumeFromWhere consumeFromWhere =
                this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getConsumeFromWhere();
        final OffsetStore offsetStore = this.defaultMQPushConsumerImpl.getOffsetStore();
        switch (consumeFromWhere) {
        case CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST:
        case CONSUME_FROM_MIN_OFFSET:
        case CONSUME_FROM_MAX_OFFSET:
        case CONSUME_FROM_LAST_OFFSET: {
            long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
            if (lastOffset >= 0) {
                result = lastOffset;
            }
            // First start,no offset
            else if (-1 == lastOffset) {
                if (mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    result = 0L;
                }
                else {
                    try {
                        result = this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
                    }
                    catch (MQClientException e) {
                        result = -1;
                    }
                }
            }
            else {
                result = -1;
            }
            break;
        }
        case CONSUME_FROM_FIRST_OFFSET: {
            long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
            if (lastOffset >= 0) {
                result = lastOffset;
            }
            else if (-1 == lastOffset) {
                result = 0L;
            }
            else {
                result = -1;
            }
            break;
        }
        case CONSUME_FROM_TIMESTAMP: {
            long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
            if (lastOffset >= 0) {
                result = lastOffset;
            }
            else if (-1 == lastOffset) {
                if (mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    try {
                        result = this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
                    }
                    catch (MQClientException e) {
                        result = -1;
                    }
                }
                else {
                    try {
                        long timestamp =
                                UtilAll.parseDate(
                                    this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer()
                                        .getConsumeTimestamp(), UtilAll.yyyyMMddHHmmss).getTime();
                        result = this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
                    }
                    catch (MQClientException e) {
                        result = -1;
                    }
                }
            }
            else {
                result = -1;
            }
            break;
        }

        default:
            break;
        }

        return result;
    }


    @Override
    public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
    }


    @Override
    public boolean removeUnnecessaryMessageQueue(MessageQueue mq, ProcessQueue pq) {
        this.defaultMQPushConsumerImpl.getOffsetStore().persist(mq);
        this.defaultMQPushConsumerImpl.getOffsetStore().removeOffset(mq);
        if (this.defaultMQPushConsumerImpl.isConsumeOrderly()
                && MessageModel.CLUSTERING.equals(this.defaultMQPushConsumerImpl.messageModel())) {
            try {
                if (pq.getLockConsume().tryLock(1000, TimeUnit.MILLISECONDS)) {
                    try {
                        this.unlock(mq, true);
                        return true;
                    }
                    finally {
                        pq.getLockConsume().unlock();
                    }
                }
                else {
                    log.warn(
                        "[WRONG]mq is consuming, so can not unlock it, {}. maybe hanged for a while, {}",//
                        mq,//
                        pq.getTryUnlockTimes());

                    pq.incTryUnlockTimes();
                }
            }
            catch (Exception e) {
                log.error("removeUnnecessaryMessageQueue Exception", e);
            }

            return false;
        }
        return true;
    }


    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_PASSIVELY;
    }
}
