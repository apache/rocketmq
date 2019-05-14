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
import io.openmessaging.consumer.BatchMessageListener;
import io.openmessaging.consumer.Consumer;
import io.openmessaging.consumer.MessageListener;
import io.openmessaging.consumer.MessageReceipt;
import io.openmessaging.exception.OMSRuntimeException;
import io.openmessaging.extension.Extension;
import io.openmessaging.extension.QueueMetaData;
import io.openmessaging.interceptor.ConsumerInterceptor;
import io.openmessaging.internal.DefaultKeyValue;
import io.openmessaging.message.Message;
import io.openmessaging.rocketmq.config.ClientConfig;
import io.openmessaging.rocketmq.domain.BytesMessageImpl;
import io.openmessaging.rocketmq.domain.ConsumeRequest;
import io.openmessaging.rocketmq.utils.BeanUtils;
import io.openmessaging.rocketmq.utils.OMSUtil;
import org.apache.rocketmq.client.consumer.*;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.LanguageCode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class PullConsumerImpl implements Consumer {
    private final DefaultMQPullConsumer rocketmqPullConsumer;
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
//        properties.put(OMSBuiltinKeys.CONSUMER_ID, consumerId);
        properties.put("TIMEOUT", consumerId);

        this.rocketmqPullConsumer.setLanguage(LanguageCode.OMS);

        this.localMessageCache = new LocalMessageCache(this.rocketmqPullConsumer, clientConfig);

        consumerInterceptors = new ArrayList<>(16);
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

    @Override
    public void resume() {
        currentState = ServiceLifeState.STARTED;
    }

    @Override
    public void suspend() {
        currentState = ServiceLifeState.STOPPED;
    }

    @Override
    public void suspend(long timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSuspended() {
        if (ServiceLifeState.STOPPED.equals(currentState)) {
            return true;
        }
        return false;
    }

    @Override
    public void bindQueue(String queueName) {
        registerPullTaskCallback(queueName);
    }

    @Override
    public void bindQueue(List<String> queueNames) {
        for (String queueName : queueNames) {
            bindQueue(queueName);
        }
    }

    @Override
    public void bindQueue(String queueName, MessageListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void bindQueues(List<String> queueNames, MessageListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void bindQueue(String queueName, BatchMessageListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void bindQueues(List<String> queueNames, BatchMessageListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unbindQueue(String queueName) {
        this.rocketmqPullConsumer.getRegisterTopics().remove(queueName);
    }

    @Override
    public void unbindQueues(List<String> queueNames) {
        for (String queueName : queueNames) {
            this.rocketmqPullConsumer.getRegisterTopics().remove(queueName);
        }
    }

    @Override
    public boolean isBindQueue() {
        Set<String> registerTopics = rocketmqPullConsumer.getRegisterTopics();
        if (null == registerTopics || registerTopics.isEmpty()) {
            return false;
        }
        return true;
    }

    @Override
    public List<String> getBindQueues() {
        Set<String> registerTopics = rocketmqPullConsumer.getRegisterTopics();
        return new ArrayList<>(registerTopics);
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
        KeyValue properties = new DefaultKeyValue();
        properties.put("TIMEOUT", timeout);
        MessageExt rmqMsg = localMessageCache.poll(properties);
        return rmqMsg == null ? null : OMSUtil.msgConvert(rmqMsg);
    }

    @Override
    public Message receive(String queueName, int partitionId, long receiptId, long timeout) {
        KeyValue properties = new DefaultKeyValue();
        properties.put("QUEUE_NAME", queueName);
        properties.put("PARTITION_ID", partitionId);
        properties.put("RECEIPT_ID", receiptId);
        properties.put("TIMEOUT", timeout);
        MessageExt rmqMsg = localMessageCache.poll(properties);
        return rmqMsg == null ? null : OMSUtil.msgConvert(rmqMsg);
    }

    @Override
    public List<Message> batchReceive(long timeout) {
        KeyValue properties = new DefaultKeyValue();
        properties.put("TIMEOUT", timeout);
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
    public List<Message> batchReceive(String queueName, int partitionId, long receiptId, long timeout) {
        MessageQueue mq = null;
        try {
            Set<MessageQueue> messageQueues = rocketmqPullConsumer.fetchSubscribeMessageQueues(queueName);
            for (MessageQueue messageQueue : messageQueues) {
                if (messageQueue.getQueueId() == partitionId) {
                    mq = messageQueue;
                }
            }
        } catch (MQClientException e) {
            e.printStackTrace();
        }
        PullResult pullResult;
        try {
            pullResult = rocketmqPullConsumer.pull(mq, "*", receiptId, 4 * 1024 * 1024, timeout);
        } catch (MQClientException e) {
            e.printStackTrace();
            return null;
        } catch (RemotingException e) {
            e.printStackTrace();
            return null;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        } catch (MQBrokerException e) {
            e.printStackTrace();
            return null;
        }
        if (null == pullResult) {
            return null;
        }
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
        }
        return null;
    }

    @Override
    public void ack(MessageReceipt receipt) {

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
    public void start() {
        if (!started) {
            try {
                this.pullConsumerScheduleService.start();
                this.localMessageCache.start();
            } catch (MQClientException e) {
                throw new OMSRuntimeException(-1, e);
            }
        }
        this.started = true;
        currentState = ServiceLifeState.STARTED;
    }

    @Override
    public void stop() {
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
    public QueueMetaData getQueueMetaData(String queueName) {
        return localMessageCache.getQueueMetaData(queueName);
    }
}
