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

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.OMS;
import io.openmessaging.OMSBuiltinKeys;
import io.openmessaging.consumer.MessageListener;
import io.openmessaging.consumer.PushConsumer;
import io.openmessaging.exception.OMSRuntimeException;
import io.openmessaging.interceptor.ConsumerInterceptor;
import io.openmessaging.rocketmq.config.ClientConfig;
import io.openmessaging.rocketmq.domain.NonStandardKeys;
import io.openmessaging.rocketmq.utils.BeanUtils;
import io.openmessaging.rocketmq.utils.OMSUtil;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.protocol.LanguageCode;

public class PushConsumerImpl implements PushConsumer {
    private final DefaultMQPushConsumer rocketmqPushConsumer;
    private final KeyValue properties;
    private boolean started = false;
    private final Map<String, MessageListener> subscribeTable = new ConcurrentHashMap<>();
    private final ClientConfig clientConfig;

    public PushConsumerImpl(final KeyValue properties) {
        this.rocketmqPushConsumer = new DefaultMQPushConsumer();
        this.properties = properties;
        this.clientConfig = BeanUtils.populate(properties, ClientConfig.class);

        if ("true".equalsIgnoreCase(System.getenv("OMS_RMQ_DIRECT_NAME_SRV"))) {
            String accessPoints = clientConfig.getAccessPoints();
            if (accessPoints == null || accessPoints.isEmpty()) {
                throw new OMSRuntimeException("-1", "OMS AccessPoints is null or empty.");
            }
            this.rocketmqPushConsumer.setNamesrvAddr(accessPoints.replace(',', ';'));
        }

        String consumerGroup = clientConfig.getConsumerId();
        if (null == consumerGroup || consumerGroup.isEmpty()) {
            throw new OMSRuntimeException("-1", "Consumer Group is necessary for RocketMQ, please set it.");
        }
        this.rocketmqPushConsumer.setConsumerGroup(consumerGroup);
        this.rocketmqPushConsumer.setMaxReconsumeTimes(clientConfig.getRmqMaxRedeliveryTimes());
        this.rocketmqPushConsumer.setConsumeTimeout(clientConfig.getRmqMessageConsumeTimeout());
        this.rocketmqPushConsumer.setConsumeThreadMax(clientConfig.getRmqMaxConsumeThreadNums());
        this.rocketmqPushConsumer.setConsumeThreadMin(clientConfig.getRmqMinConsumeThreadNums());

        String consumerId = OMSUtil.buildInstanceName();
        this.rocketmqPushConsumer.setInstanceName(consumerId);
        properties.put(OMSBuiltinKeys.CONSUMER_ID, consumerId);
        this.rocketmqPushConsumer.setLanguage(LanguageCode.OMS);

        this.rocketmqPushConsumer.registerMessageListener(new MessageListenerImpl());
    }

    @Override
    public KeyValue attributes() {
        return properties;
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

    }

    @Override
    public boolean isSuspended() {
        return this.rocketmqPushConsumer.isPause();
    }

    @Override
    public PushConsumer attachQueue(final String queueName, final MessageListener listener) {
        this.subscribeTable.put(queueName, listener);
        try {
            this.rocketmqPushConsumer.subscribe(queueName, "*");
        } catch (MQClientException e) {
            throw new OMSRuntimeException("-1", String.format("RocketMQ push consumer can't attach to %s.", queueName));
        }
        return this;
    }

    @Override
    public PushConsumer attachQueue(String queueName, MessageListener listener, KeyValue attributes) {
        return this.attachQueue(queueName, listener);
    }

    @Override
    public PushConsumer detachQueue(String queueName) {
        this.subscribeTable.remove(queueName);
        try {
            this.rocketmqPushConsumer.unsubscribe(queueName);
        } catch (Exception e) {
            throw new OMSRuntimeException("-1", String.format("RocketMQ push consumer fails to unsubscribe topic: %s", queueName));
        }
        return null;
    }

    @Override
    public void addInterceptor(ConsumerInterceptor interceptor) {

    }

    @Override
    public void removeInterceptor(ConsumerInterceptor interceptor) {

    }

    @Override
    public synchronized void startup() {
        if (!started) {
            try {
                this.rocketmqPushConsumer.start();
            } catch (MQClientException e) {
                throw new OMSRuntimeException("-1", e);
            }
        }
        this.started = true;
    }

    @Override
    public synchronized void shutdown() {
        if (this.started) {
            this.rocketmqPushConsumer.shutdown();
        }
        this.started = false;
    }

    class MessageListenerImpl implements MessageListenerConcurrently {

        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> rmqMsgList,
            ConsumeConcurrentlyContext contextRMQ) {
            MessageExt rmqMsg = rmqMsgList.get(0);
            BytesMessage omsMsg = OMSUtil.msgConvert(rmqMsg);

            MessageListener listener = PushConsumerImpl.this.subscribeTable.get(rmqMsg.getTopic());

            if (listener == null) {
                throw new OMSRuntimeException("-1",
                    String.format("The topic/queue %s isn't attached to this consumer", rmqMsg.getTopic()));
            }

            final KeyValue contextProperties = OMS.newKeyValue();
            final CountDownLatch sync = new CountDownLatch(1);

            contextProperties.put(NonStandardKeys.MESSAGE_CONSUME_STATUS, ConsumeConcurrentlyStatus.RECONSUME_LATER.name());

            MessageListener.Context context = new MessageListener.Context() {
                @Override
                public KeyValue attributes() {
                    return contextProperties;
                }

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

            return ConsumeConcurrentlyStatus.valueOf(contextProperties.getString(NonStandardKeys.MESSAGE_CONSUME_STATUS));
        }
    }
}
