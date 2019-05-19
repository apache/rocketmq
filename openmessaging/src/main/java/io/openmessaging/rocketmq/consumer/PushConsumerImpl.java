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
package io.openmessaging.rocketmq.consumer;

import io.openmessaging.KeyValue;
import io.openmessaging.OMS;
import io.openmessaging.ServiceLifeState;
import io.openmessaging.consumer.BatchMessageListener;
import io.openmessaging.consumer.Consumer;
import io.openmessaging.consumer.MessageListener;
import io.openmessaging.consumer.MessageReceipt;
import io.openmessaging.exception.OMSRuntimeException;
import io.openmessaging.extension.Extension;
import io.openmessaging.extension.QueueMetaData;
import io.openmessaging.interceptor.ConsumerInterceptor;
import io.openmessaging.message.Message;
import io.openmessaging.rocketmq.config.ClientConfig;
import io.openmessaging.rocketmq.config.DefaultQueueMetaData;
import io.openmessaging.rocketmq.domain.BytesMessageImpl;
import io.openmessaging.rocketmq.domain.NonStandardKeys;
import io.openmessaging.rocketmq.utils.BeanUtils;
import io.openmessaging.rocketmq.utils.OMSUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.protocol.LanguageCode;

public class PushConsumerImpl implements Consumer {

    private final static InternalLogger log = ClientLogger.getLog();

    private final DefaultMQPushConsumer rocketmqPushConsumer;
    private final KeyValue properties;
    private boolean started = false;
    private final Map<String, MessageListener> subscribeTable = new ConcurrentHashMap<>();
    private final Map<String, BatchMessageListener> batchSubscribeTable = new ConcurrentHashMap<>();
    private final ClientConfig clientConfig;
    private ServiceLifeState currentState;
    private List<ConsumerInterceptor> consumerInterceptors;

    public PushConsumerImpl(final KeyValue properties) {
        this.rocketmqPushConsumer = new DefaultMQPushConsumer();
        this.properties = properties;
        this.clientConfig = BeanUtils.populate(properties, ClientConfig.class);

        if ("true".equalsIgnoreCase(System.getenv("OMS_RMQ_DIRECT_NAME_SRV"))) {
            String accessPoints = clientConfig.getAccessPoints();
            if (accessPoints == null || accessPoints.isEmpty()) {
                throw new OMSRuntimeException(-1, "OMS AccessPoints is null or empty.");
            }
            this.rocketmqPushConsumer.setNamesrvAddr(accessPoints.replace(',', ';'));
        }

        String consumerGroup = clientConfig.getConsumerId();
        if (null == consumerGroup || consumerGroup.isEmpty()) {
            throw new OMSRuntimeException(-1, "Consumer Group is necessary for RocketMQ, please set it.");
        }
        this.rocketmqPushConsumer.setConsumerGroup(consumerGroup);
        this.rocketmqPushConsumer.setMaxReconsumeTimes(clientConfig.getRmqMaxRedeliveryTimes());
        this.rocketmqPushConsumer.setConsumeTimeout(clientConfig.getRmqMessageConsumeTimeout());
        this.rocketmqPushConsumer.setConsumeThreadMax(clientConfig.getRmqMaxConsumeThreadNums());
        this.rocketmqPushConsumer.setConsumeThreadMin(clientConfig.getRmqMinConsumeThreadNums());

        String consumerId = OMSUtil.buildInstanceName();
        this.rocketmqPushConsumer.setInstanceName(consumerId);
        properties.put(NonStandardKeys.CONSUMER_ID, consumerId);
        this.rocketmqPushConsumer.setLanguage(LanguageCode.OMS);

        this.rocketmqPushConsumer.registerMessageListener(new MessageListenerImpl());

        consumerInterceptors = new ArrayList<>(16);
        currentState = ServiceLifeState.INITIALIZED;
    }

    @Override
    public void resume() {
        this.rocketmqPushConsumer.resume();
    }

    @Override
    public void suspend() {
        this.rocketmqPushConsumer.suspend();
    }

    @Override
    public void suspend(long timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSuspended() {
        return this.rocketmqPushConsumer.getDefaultMQPushConsumerImpl().isPause();
    }

    @Override
    public void bindQueue(String queueName) {
        try {
            rocketmqPushConsumer.subscribe(queueName, "*");
        } catch (MQClientException e) {
            throw new OMSRuntimeException(-1, String.format("RocketMQ push consumer can't attach to %s.", queueName));
        }
    }

    @Override
    public void bindQueue(List<String> queueNames) {
        for (String queueName : queueNames) {
            bindQueue(queueName);
        }
    }

    @Override
    public void bindQueue(String queueName, MessageListener listener) {
        this.subscribeTable.put(queueName, listener);
        this.batchSubscribeTable.remove(queueName);
        try {
            this.rocketmqPushConsumer.subscribe(queueName, "*");
        } catch (MQClientException e) {
            throw new OMSRuntimeException(-1, String.format("RocketMQ push consumer can't attach to %s.", queueName));
        }
    }

    @Override
    public void bindQueues(List<String> queueNames, MessageListener listener) {
        for (String queueName : queueNames) {
            bindQueue(queueName, listener);
        }
    }

    @Override
    public void bindQueue(String queueName, BatchMessageListener listener) {
        this.batchSubscribeTable.put(queueName, listener);
        this.subscribeTable.remove(queueName);
        try {
            this.rocketmqPushConsumer.subscribe(queueName, "*");
        } catch (MQClientException e) {
            throw new OMSRuntimeException(-1, String.format("RocketMQ push consumer can't attach to %s.", queueName));
        }
    }

    @Override
    public void bindQueues(List<String> queueNames, BatchMessageListener listener) {
        for (String queueName : queueNames) {
            bindQueue(queueName, listener);
        }
    }

    @Override
    public void unbindQueue(String queueName) {
        this.subscribeTable.remove(queueName);
        this.batchSubscribeTable.remove(queueName);
        try {
            this.rocketmqPushConsumer.unsubscribe(queueName);
        } catch (Exception e) {
            throw new OMSRuntimeException(-1, String.format("RocketMQ push consumer fails to unsubscribe topic: %s", queueName));
        }
    }

    @Override
    public void unbindQueues(List<String> queueNames) {
        for (String queueName : queueNames) {
            unbindQueue(queueName);
        }
    }

    @Override
    public boolean isBindQueue() {
        Map<String, String> subscription = rocketmqPushConsumer.getSubscription();
        if (null != subscription && subscription.size() > 0) {
            return true;
        }
        return false;
    }

    @Override
    public List<String> getBindQueues() {
        Map<String, String> subscription = rocketmqPushConsumer.getSubscription();
        if (null != subscription && subscription.size() > 0) {
            return new ArrayList<>(subscription.keySet());
        }
        return null;
    }

    @Override
    public void addInterceptor(ConsumerInterceptor interceptor) {
        consumerInterceptors.add(interceptor);
    }

    @Override
    public void removeInterceptor(ConsumerInterceptor interceptor) {
        consumerInterceptors.remove(interceptor);
    }

    @Override
    public Message receive(long timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Message receive(String queueName, int partitionId, long receiptId, long timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Message> batchReceive(long timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Message> batchReceive(String queueName, int partitionId, long receiptId, long timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void ack(MessageReceipt receipt) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Extension> getExtension() {
        return Optional.of(new Extension() {

            @Override
            public QueueMetaData getQueueMetaData(String queueName) {
                return getQueueMetaData(queueName);
            }
        });
    }

    @Override
    public synchronized void start() {
        currentState = ServiceLifeState.STARTING;
        if (!started) {
            try {
                this.rocketmqPushConsumer.start();
            } catch (MQClientException e) {
                throw new OMSRuntimeException(-1, e);
            }
        }
        this.started = true;
        currentState = ServiceLifeState.STARTED;
    }

    @Override
    public synchronized void stop() {
        currentState = ServiceLifeState.STOPPING;
        if (this.started) {
            this.rocketmqPushConsumer.shutdown();
        }
        this.started = false;
        currentState = ServiceLifeState.STOPPED;
    }

    @Override
    public ServiceLifeState currentState() {
        return currentState;
    }

    @Override
    public QueueMetaData getQueueMetaData(String queueName) {
        Set<MessageQueue> messageQueues;
        try {
            messageQueues = rocketmqPushConsumer.fetchSubscribeMessageQueues(queueName);
        } catch (MQClientException e) {
            log.error("A error occurred when get queue metadata.", e);
            return null;
        }
        List<QueueMetaData.Partition> partitions = new ArrayList<>(16);
        if (null != messageQueues && !messageQueues.isEmpty()) {
            for (MessageQueue messageQueue : messageQueues) {
                QueueMetaData.Partition partition = new DefaultQueueMetaData.DefaultPartition(messageQueue.getQueueId(), messageQueue.getBrokerName());
                partitions.add(partition);
            }
        } else {
            return null;
        }
        QueueMetaData queueMetaData = new DefaultQueueMetaData(queueName, partitions);
        return queueMetaData;
    }

    class MessageListenerImpl implements MessageListenerConcurrently {

        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> rmqMsgList,
                                                        ConsumeConcurrentlyContext contextRMQ) {
            boolean batchFlag = true;
            MessageExt rmqMsg = rmqMsgList.get(0);
            BatchMessageListener batchMessageListener = PushConsumerImpl.this.batchSubscribeTable.get(rmqMsg.getTopic());
            MessageListener listener = PushConsumerImpl.this.subscribeTable.get(rmqMsg.getTopic());
            if (null == batchMessageListener) {
                batchFlag = false;
            }
            if (listener == null && batchMessageListener == null) {
                throw new OMSRuntimeException(-1,
                        String.format("The topic/queue %s isn't attached to this consumer", rmqMsg.getTopic()));
            }
            final KeyValue contextProperties = OMS.newKeyValue();

            if (batchFlag) {
                List<Message> messages = new ArrayList<>(16);
                for (MessageExt messageExt : rmqMsgList) {
                    BytesMessageImpl omsMsg = OMSUtil.msgConvert(messageExt);
                    messages.add(omsMsg);
                }
                final CountDownLatch sync = new CountDownLatch(1);

                contextProperties.put(NonStandardKeys.MESSAGE_CONSUME_STATUS, ConsumeConcurrentlyStatus.RECONSUME_LATER.name());

                BatchMessageListener.Context context = new BatchMessageListener.Context() {

                    @Override
                    public void success(MessageReceipt... messages) {

                    }

                    @Override
                    public void ack() {
                        sync.countDown();
                        contextProperties.put(NonStandardKeys.MESSAGE_CONSUME_STATUS,
                                ConsumeConcurrentlyStatus.CONSUME_SUCCESS.name());
                    }
                };
                long begin = System.currentTimeMillis();
                batchMessageListener.onReceived(messages, context);
                long costs = System.currentTimeMillis() - begin;
                long timeoutMills = clientConfig.getRmqMessageConsumeTimeout() * 60 * 1000;
                try {
                    sync.await(Math.max(0, timeoutMills - costs), TimeUnit.MILLISECONDS);
                } catch (InterruptedException ignore) {
                }
            } else {
                BytesMessageImpl omsMsg = OMSUtil.msgConvert(rmqMsg);

                final CountDownLatch sync = new CountDownLatch(1);

                contextProperties.put(NonStandardKeys.MESSAGE_CONSUME_STATUS, ConsumeConcurrentlyStatus.RECONSUME_LATER.name());

                MessageListener.Context context = new MessageListener.Context() {

                    @Override
                    public void ack() {
                        sync.countDown();
                        contextProperties.put(NonStandardKeys.MESSAGE_CONSUME_STATUS,
                                ConsumeConcurrentlyStatus.CONSUME_SUCCESS.name());
                    }
                };
                long begin = System.currentTimeMillis();
                listener.onReceived(omsMsg, context);
                long costs = System.currentTimeMillis() - begin;
                long timeoutMills = clientConfig.getRmqMessageConsumeTimeout() * 60 * 1000;
                try {
                    sync.await(Math.max(0, timeoutMills - costs), TimeUnit.MILLISECONDS);
                } catch (InterruptedException ignore) {
                }
            }

            return ConsumeConcurrentlyStatus.valueOf(contextProperties.getString(NonStandardKeys.MESSAGE_CONSUME_STATUS));
        }
    }
}
