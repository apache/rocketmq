/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.consumer;

import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.RPCHook;

public class MQConsumerBuilder {

    /**
     * NameServer addresses to which this MQ Producer instance connects
     */
    private String nameserverAddress;

    /**
     * Namespace for this MQ Producer instance.
     */
    private String namespace;

    /**
     * Consumers belonging to the same consumer group share a group id. The consumers in a group then divides the topic
     * as fairly amongst themselves as possible by establishing that each queue is only consumed by a single consumer
     * from the group. If all consumers are from the same group, it functions as a traditional message queue. Each
     * message would be consumed by one consumer of the group only. When multiple consumer groups exist, the flow of the
     * data consumption model aligns with the traditional publish-subscribe model. The messages are broadcast to all
     * consumer groups.
     */
    private String consumerGroup = MixAll.DEFAULT_CONSUMER_GROUP;

    /**
     * Queue allocation algorithm specifying how message queues are allocated to each consumer clients.
     */
    private AllocateMessageQueueStrategy allocateMessageQueueStrategy = new AllocateMessageQueueAveragely();

    /**
     * RPC hook to execute before each remoting command.
     */
    private RPCHook rpcHook;

    /**
     * Switch flag instance for message trace.
     */
    private boolean enableMessageTrace = false;

    /**
     * The name value of message trace topic.If you don't config,you can use the default trace topic name.
     */
    private String customizedTraceTopic;

    /**
     * Consuming point on consumer booting.
     * </p>
     * <p>
     * There are three consuming points:
     * <ul>
     * <li>
     * <code>CONSUME_FROM_LAST_OFFSET</code>: consumer clients pick up where it stopped previously.
     * If it were a newly booting up consumer client, according aging of the consumer group, there are two
     * cases:
     * <ol>
     * <li>
     * if the consumer group is created so recently that the earliest message being subscribed has yet
     * expired, which means the consumer group represents a lately launched business, consuming will
     * start from the very beginning;
     * </li>
     * <li>
     * if the earliest message being subscribed has expired, consuming will start from the latest
     * messages, meaning messages born prior to the booting timestamp would be ignored.
     * </li>
     * </ol>
     * </li>
     * <li>
     * <code>CONSUME_FROM_FIRST_OFFSET</code>: Consumer client will start from earliest messages available.
     * </li>
     * <li>
     * <code>CONSUME_FROM_TIMESTAMP</code>: Consumer client will start from specified timestamp, which means
     * messages born prior to {@link #consumeTimestamp} will be ignored
     * </li>
     * </ul>
     */
    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;

    /**
     * Backtracking consumption time with second precision. Time format is
     * 20131223171201<br>
     * Implying Seventeen twelve and 01 seconds on December 23, 2013 year<br>
     * Default backtracking consumption time Half an hour ago.
     */
    private String consumeTimestamp;

    /**
     * consumer thread number
     */
    private int consumeThreadNum = 20;

    /**
     * Instance name, also set by rocketmq.client.name
     */
    private String instanceName;

    /**
     * Message model defines the way how messages are delivered to each consumer clients.
     * </p>
     * <p>
     * RocketMQ supports two message models: clustering and broadcasting. If clustering is set, consumer clients with
     * the same {@link #consumerGroup} would only consume shards of the messages subscribed, which achieves load
     * balances; Conversely, if the broadcasting is set, each consumer client will consume all subscribed messages
     * separately.
     * </p>
     * <p>
     * This field defaults to clustering.
     */
    private MessageModel messageModel = MessageModel.CLUSTERING;

    /**
     * Max re-consume times.
     * In concurrently mode, -1 means 16;
     * In orderly mode, -1 means Integer.MAX_VALUE.
     * <p>
     * If messages are re-consumed more than {@link #maxReconsumeTimes} before success.
     */
    private int maxReconsumeTimes = -1;

    /**
     * Maximum amount of time in minutes a message may block the consuming thread.
     */
    private long consumeTimeoutMinutes = 15;


    /**
     * Create a DefaultMQPushConsumer
     *
     * @return DefaultMQPushConsumer
     */
    public DefaultMQPushConsumer createPushConsumer() {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(this.namespace, this.consumerGroup, this.rpcHook,
                this.allocateMessageQueueStrategy, this.enableMessageTrace, this.customizedTraceTopic);

        if (null != nameserverAddress && !"".equals(nameserverAddress)) {
            consumer.setNamesrvAddr(nameserverAddress);
        }

        consumer.setConsumeFromWhere(consumeFromWhere);
        if (null != consumeTimestamp && !"".equals(consumeTimestamp)) {
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP);
            consumer.setConsumeTimestamp(consumeTimestamp);
        } else if (ConsumeFromWhere.CONSUME_FROM_TIMESTAMP.equals(this.consumeFromWhere)) {
            consumer.setConsumeTimestamp(UtilAll.timeMillisToHumanString3(System.currentTimeMillis() - (1000 * 60 * 30)));
        }

        consumer.setConsumeThreadMax(this.consumeThreadNum);
        consumer.setConsumeThreadMin(this.consumeThreadNum);
        if (null != this.instanceName && !"".equals(this.instanceName)) {
            consumer.setInstanceName(this.instanceName);
        }
        consumer.setMessageModel(this.messageModel);
        consumer.setMaxReconsumeTimes(this.maxReconsumeTimes);
        consumer.setConsumeTimeout(this.consumeTimeoutMinutes);

        return consumer;
    }

    public DefaultLitePullConsumer createLitePullConsumer() {
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer(this.namespace, this.consumerGroup, this.rpcHook);

        if (null != nameserverAddress && !"".equals(nameserverAddress)) {
            consumer.setNamesrvAddr(nameserverAddress);
        }

        consumer.setConsumeFromWhere(consumeFromWhere);
        if (null != consumeTimestamp && !"".equals(consumeTimestamp)) {
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP);
            consumer.setConsumeTimestamp(consumeTimestamp);
        }
        consumer.setPullThreadNums(this.consumeThreadNum);

        if (null != this.instanceName && !"".equals(this.instanceName)) {
            consumer.setInstanceName(this.instanceName);
        }
        consumer.setMessageModel(this.messageModel);

        if (null != this.allocateMessageQueueStrategy) {
            consumer.setAllocateMessageQueueStrategy(this.allocateMessageQueueStrategy);
        }
        consumer.setEnableMsgTrace(this.enableMessageTrace);

        if (this.enableMessageTrace && null != this.customizedTraceTopic && !"".equals(this.customizedTraceTopic)) {
            consumer.setCustomizedTraceTopic(this.customizedTraceTopic);
        }

        return consumer;
    }

    public MQConsumerBuilder nameserverAddress(final String nameserverAddr) {
        this.nameserverAddress = nameserverAddr;
        return this;
    }

    public MQConsumerBuilder namespace(final String namespace) {
        this.namespace = namespace;
        return this;
    }

    public MQConsumerBuilder consumerGroup(final String consumerGroup) {
        if (null != consumerGroup && !"".equals(consumerGroup)) {
            this.consumerGroup = consumerGroup;
        }
        return this;
    }

    public MQConsumerBuilder rpcHook(final RPCHook rpcHook) {
        this.rpcHook = rpcHook;
        return this;
    }

    public MQConsumerBuilder allocateMessageQueueStrategy(final AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        if (null != allocateMessageQueueStrategy) {
            this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        }
        return this;
    }

    public MQConsumerBuilder enableMessageTrace(final String customizedTraceTopic) {
        if (null != customizedTraceTopic && !"".equals(customizedTraceTopic)) {
            this.enableMessageTrace = true;
            this.customizedTraceTopic = customizedTraceTopic;
        }
        return this;
    }

    public MQConsumerBuilder consumeFrom(final ConsumeFromWhere consumeFromWhere) {
        if (null != consumeFromWhere) {
            this.consumeFromWhere = consumeFromWhere;
        }
        return this;
    }

    public MQConsumerBuilder consumeTimeStamp(final String consumeTimestamp) {
        if (null != consumeTimestamp && !"".equals(consumeTimestamp)) {
            this.consumeTimestamp = consumeTimestamp;
        }
        return this;
    }

    public MQConsumerBuilder consumeThreadNum(final int consumeThreadNum) {
        if (consumeThreadNum > 0) {
            this.consumeThreadNum = consumeThreadNum;
        }
        return this;
    }

    public MQConsumerBuilder instanceName(final String instanceName) {
        this.instanceName = instanceName;
        return this;
    }

    public MQConsumerBuilder messageModel(final MessageModel messageModel) {
        if (null != messageModel) {
            this.messageModel = messageModel;
        }
        return this;
    }

    public MQConsumerBuilder maxReconsumeTimes(final int maxReconsumeTimes) {
        this.maxReconsumeTimes = maxReconsumeTimes;
        return this;
    }

    /**
     * Only for MQPushedConsumer
     *
     * @param consumeTimeoutMinutes
     * @return
     */
    public MQConsumerBuilder consumeTimeoutMinutes(final long consumeTimeoutMinutes) {
        this.consumeTimeoutMinutes = consumeTimeoutMinutes;
        return this;
    }


}
