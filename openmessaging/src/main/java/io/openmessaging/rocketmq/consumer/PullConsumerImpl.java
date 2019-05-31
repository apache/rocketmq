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
import io.openmessaging.ServiceLifeState;
import io.openmessaging.consumer.MessageReceipt;
import io.openmessaging.consumer.PullConsumer;
import io.openmessaging.exception.OMSRuntimeException;
import io.openmessaging.extension.Extension;
import io.openmessaging.extension.QueueMetaData;
import io.openmessaging.interceptor.ConsumerInterceptor;
import io.openmessaging.internal.DefaultKeyValue;
import io.openmessaging.message.Message;
import io.openmessaging.rocketmq.config.ClientConfig;
import io.openmessaging.rocketmq.domain.BytesMessageImpl;
import io.openmessaging.rocketmq.domain.ConsumeRequest;
import io.openmessaging.rocketmq.domain.NonStandardKeys;
import io.openmessaging.rocketmq.utils.BeanUtils;
import io.openmessaging.rocketmq.utils.OMSUtil;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.rocketmq.client.MQAdmin;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.MQPullConsumer;
import org.apache.rocketmq.client.consumer.MQPullConsumerScheduleService;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullTaskCallback;
import org.apache.rocketmq.client.consumer.PullTaskContext;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.LanguageCode;

public class PullConsumerImpl implements PullConsumer {

    private static final int PULL_MAX_NUMS = 32;
    private static final int PULL_MIN_NUMS = 1;

    private final DefaultMQPullConsumer rocketmqPullConsumer;
    private final MQAdmin mqAdmin;
    private final KeyValue properties;
    private boolean started = false;
    private final MQPullConsumerScheduleService pullConsumerScheduleService;
    private final LocalMessageCache localMessageCache;
    private final ClientConfig clientConfig;
    private ServiceLifeState currentState;
    private List<ConsumerInterceptor> consumerInterceptors;

    private final static InternalLogger log = ClientLogger.getLog();

    public PullConsumerImpl(final KeyValue properties) {
        this.properties = properties;
        this.clientConfig = BeanUtils.populate(properties, ClientConfig.class);

        String consumerGroup = clientConfig.getConsumerId();
        if (null == consumerGroup || consumerGroup.isEmpty()) {
            throw new OMSRuntimeException(-1, "Consumer Group is necessary for RocketMQ, please set it.");
        }
        pullConsumerScheduleService = new MQPullConsumerScheduleService(consumerGroup);

        this.rocketmqPullConsumer = pullConsumerScheduleService.getDefaultMQPullConsumer();

        if ("true".equalsIgnoreCase(System.getenv("OMS_RMQ_DIRECT_NAME_SRV"))) {
            String accessPoints = clientConfig.getAccessPoints();
            if (accessPoints == null || accessPoints.isEmpty()) {
                throw new OMSRuntimeException(-1, "OMS AccessPoints is null or empty.");
            }
            this.rocketmqPullConsumer.setNamesrvAddr(accessPoints.replace(',', ';'));
        }

        this.rocketmqPullConsumer.setConsumerGroup(consumerGroup);

        int maxReDeliveryTimes = clientConfig.getRmqMaxRedeliveryTimes();
        this.rocketmqPullConsumer.setMaxReconsumeTimes(maxReDeliveryTimes);

        String consumerId = OMSUtil.buildInstanceName();
        this.rocketmqPullConsumer.setInstanceName(consumerId);
        properties.put(NonStandardKeys.CONSUMER_ID, consumerId);

        this.rocketmqPullConsumer.setLanguage(LanguageCode.OMS);
//        this.rocketmqPullConsumer.setNamespace();

        this.localMessageCache = new LocalMessageCache(this.rocketmqPullConsumer, clientConfig);

        consumerInterceptors = new ArrayList<>(16);

        mqAdmin = rocketmqPullConsumer;
//        mqAdmin.createTopic();
    }

    private void registerPullTaskCallback(final String targetQueueName) {
        this.pullConsumerScheduleService.registerPullTaskCallback(targetQueueName, new PullTaskCallback() {
            @Override
            public void doPullTask(final MessageQueue mq, final PullTaskContext context) {
                MQPullConsumer consumer = context.getPullConsumer();
                try {
                    long offset = localMessageCache.nextPullOffset(mq);

                    PullResult pullResult = consumer.pull(mq, "*",
                        offset, localMessageCache.nextPullBatchNums());
                    ProcessQueue pq = rocketmqPullConsumer.getDefaultMQPullConsumerImpl().getRebalanceImpl()
                        .getProcessQueueTable().get(mq);
                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            if (pq != null) {
                                pq.putMessage(pullResult.getMsgFoundList());
                                for (final MessageExt messageExt : pullResult.getMsgFoundList()) {
                                    localMessageCache.submitConsumeRequest(new ConsumeRequest(messageExt, mq, pq));
                                }
                            }
                            break;
                        default:
                            break;
                    }
                    localMessageCache.updatePullOffset(mq, pullResult.getNextBeginOffset());
                } catch (Exception e) {
                    log.error("A error occurred in pull message process.", e);
                }
            }
        });
    }

    @Override public Set<String> getBindQueues() {
        return rocketmqPullConsumer.getRegisterTopics();
    }

    @Override
    public void addInterceptor(ConsumerInterceptor interceptor) {
        consumerInterceptors.add(interceptor);
    }

    @Override
    public void removeInterceptor(ConsumerInterceptor interceptor) {
        consumerInterceptors.remove(interceptor);
    }

    @Override public void bindQueue(Collection<String> queueNames) {
        for (String queueName : queueNames) {
            registerPullTaskCallback(queueName);
        }
    }

    @Override public void unbindQueue(Collection<String> queueNames) {
        for (String queueName : queueNames) {
            this.rocketmqPullConsumer.getRegisterTopics().remove(queueName);
        }
    }

    @Override public Message receive() {
        KeyValue properties = new DefaultKeyValue();
        MessageExt rmqMsg = localMessageCache.poll(properties);
        return rmqMsg == null ? null : OMSUtil.msgConvert(rmqMsg);
    }

    @Override
    public Message receive(long timeout) {
        KeyValue properties = new DefaultKeyValue();
        properties.put(NonStandardKeys.TIMEOUT, timeout);
        MessageExt rmqMsg = localMessageCache.poll(properties);
        return rmqMsg == null ? null : OMSUtil.msgConvert(rmqMsg);
    }

    @Override
    public Message receive(String queueName, QueueMetaData queueMetaData, MessageReceipt messageReceipt, long timeout) {
/*        MessageQueue mq;
        mq = getQueue(queueMetaData);
        PullResult pullResult = getResult(receiptId, timeout, mq, PULL_MIN_NUMS);
        if (pullResult == null)
            return null;
        PullStatus pullStatus = pullResult.getPullStatus();
        List<Message> messages = new ArrayList<>(16);
        if (PullStatus.FOUND.equals(pullStatus)) {
            List<MessageExt> rmqMsgs = pullResult.getMsgFoundList();
            if (null != rmqMsgs && !rmqMsgs.isEmpty()) {
                for (MessageExt messageExt : rmqMsgs) {
                    BytesMessageImpl bytesMessage = OMSUtil.msgConvert(messageExt);
                    messages.add(bytesMessage);
                }
                return messages.  get(0);
            }
        }*/
        return null;
    }

    private PullResult getResult(long offset, long timeout, MessageQueue mq, int maxNums) {
        PullResult pullResult;
        try {
            pullResult = rocketmqPullConsumer.pull(mq, "*", offset, maxNums, timeout);
        } catch (MQClientException e) {
            log.error("A error occurred when pull message.", e);
            return null;
        } catch (RemotingException e) {
            log.error("A error occurred when pull message.", e);
            return null;
        } catch (InterruptedException e) {
            log.error("A error occurred when pull message.", e);
            return null;
        } catch (MQBrokerException e) {
            log.error("A error occurred when pull message.", e);
            return null;
        }
        return pullResult;
    }

    private MessageQueue getQueue(QueueMetaData queueMetaData) {
        MessageQueue mq = null;
        try {
            Set<MessageQueue> messageQueues = rocketmqPullConsumer.fetchSubscribeMessageQueues(queueMetaData.queueName());
            for (MessageQueue messageQueue : messageQueues) {
                if (messageQueue.getQueueId() == queueMetaData.partitionId()) {
                    mq = messageQueue;
                }
            }
        } catch (MQClientException e) {
            log.error("A error occurred when batch pull message.", e);
        }
        return mq;
    }

    @Override
    public List<Message> batchReceive(long timeout) {
        KeyValue properties = new DefaultKeyValue();
        properties.put(NonStandardKeys.TIMEOUT, timeout);
        List<MessageExt> rmqMsgs = localMessageCache.batchPoll(properties);
        if (null != rmqMsgs && !rmqMsgs.isEmpty()) {
            List<Message> messages = new ArrayList<>(rmqMsgs.size());
            for (MessageExt messageExt : rmqMsgs) {
                BytesMessageImpl bytesMessage = OMSUtil.msgConvert(messageExt);
                messages.add(bytesMessage);
            }
            return messages;
        }
        return null;
    }

    @Override
    public List<Message> batchReceive(String queueName, QueueMetaData queueMetaData, MessageReceipt messageReceipt,
        long timeout) {
/*        MessageQueue mq;
        mq = getQueue(queueMetaData);
        PullResult pullResult = getResult(receiptId, timeout, mq, PULL_MAX_NUMS);
        if (pullResult == null)
            return null;
        PullStatus pullStatus = pullResult.getPullStatus();
        List<Message> messages = new ArrayList<>(16);
        if (PullStatus.FOUND.equals(pullStatus)) {
            List<MessageExt> rmqMsgs = pullResult.getMsgFoundList();
            if (null != rmqMsgs && !rmqMsgs.isEmpty()) {
                for (MessageExt messageExt : rmqMsgs) {
                    BytesMessageImpl bytesMessage = OMSUtil.msgConvert(messageExt);
                    messages.add(bytesMessage);
                }
                return messages;
            }
        }*/
        return null;
    }

    @Override
    public void ack(MessageReceipt receipt) {

    }

    @Override
    public Optional<Extension> getExtension() {
        return Optional.of(new Extension() {
            @Override
            public Set<QueueMetaData> getQueueMetaData(String queueName) {
                return PullConsumerImpl.this.getQueueMetaData(queueName);
            }
        });
    }

    @Override
    public synchronized void start() {
        if (!started) {
            try {
                this.pullConsumerScheduleService.start();
                this.localMessageCache.start();
            } catch (MQClientException e) {
                throw new OMSRuntimeException(-1, e);
            }
        }
        this.started = true;
    }

    @Override
    public synchronized void stop() {
        if (this.started) {
            this.localMessageCache.stop();
            this.pullConsumerScheduleService.shutdown();
            this.rocketmqPullConsumer.shutdown();
        }
        this.started = false;
    }

    @Override
    public ServiceLifeState currentState() {
        return localMessageCache.currentState();
    }

    @Override
    public Set<QueueMetaData> getQueueMetaData(String queueName) {
        return localMessageCache.getQueueMetaData(queueName);
    }
}
