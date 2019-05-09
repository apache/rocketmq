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
package io.openmessaging.rocketmq.producer;

import io.openmessaging.Future;
import io.openmessaging.KeyValue;
import io.openmessaging.ServiceLifeState;
import io.openmessaging.exception.OMSMessageFormatException;
import io.openmessaging.extension.Extension;
import io.openmessaging.extension.QueueMetaData;
import io.openmessaging.message.Message;
import io.openmessaging.Promise;
import io.openmessaging.exception.OMSRuntimeException;
import io.openmessaging.interceptor.ProducerInterceptor;
import io.openmessaging.producer.Producer;
import io.openmessaging.producer.SendResult;
import io.openmessaging.producer.TransactionalResult;
import io.openmessaging.rocketmq.domain.BytesMessageImpl;
import io.openmessaging.rocketmq.promise.DefaultPromise;
import io.openmessaging.rocketmq.utils.OMSUtil;
import java.util.List;
import java.util.Optional;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendStatus;

import static io.openmessaging.rocketmq.utils.OMSUtil.msgConvert;

public class ProducerImpl extends AbstractOMSProducer implements Producer {

    public ProducerImpl(final KeyValue properties) {
        super(properties);
    }

    @Override
    public SendResult send(final Message message) {
        return send(message, this.rocketmqProducer.getSendMsgTimeout());
    }

    private SendResult send(final Message message, long timeout) {
        checkMessageType(message);
        org.apache.rocketmq.common.message.Message rmqMessage = msgConvert((BytesMessageImpl) message);
        try {
            org.apache.rocketmq.client.producer.SendResult rmqResult = this.rocketmqProducer.send(rmqMessage, timeout);
            if (!rmqResult.getSendStatus().equals(SendStatus.SEND_OK)) {
                log.error(String.format("Send message to RocketMQ failed, %s", message));
                throw new OMSRuntimeException(-1, "Send message to RocketMQ broker failed.");
            }
            message.header().setMessageId(rmqResult.getMsgId());
            return OMSUtil.sendResultConvert(rmqResult);
        } catch (Exception e) {
            log.error(String.format("Send message to RocketMQ failed, %s", message), e);
            throw checkProducerException(rmqMessage.getTopic(), message.header().getMessageId(), e);
        }
    }

    @Override
    public Promise<SendResult> sendAsync(final Message message) {
        return sendAsync(message, this.rocketmqProducer.getSendMsgTimeout());
    }

    private Promise<SendResult> sendAsync(final Message message, long timeout) {
        checkMessageType(message);
        org.apache.rocketmq.common.message.Message rmqMessage = msgConvert((BytesMessageImpl) message);
        final Promise<SendResult> promise = new DefaultPromise<>();
        try {
            this.rocketmqProducer.send(rmqMessage, new SendCallback() {
                @Override
                public void onSuccess(final org.apache.rocketmq.client.producer.SendResult rmqResult) {
                    message.header().setMessageId(rmqResult.getMsgId());
                    promise.set(OMSUtil.sendResultConvert(rmqResult));
                }

                @Override
                public void onException(final Throwable e) {
                    promise.setFailure(e);
                }
            }, timeout);
        } catch (Exception e) {
            promise.setFailure(e);
        }
        return promise;
    }

    @Override
    public void sendOneway(final Message message) {
        checkMessageType(message);
        org.apache.rocketmq.common.message.Message rmqMessage = msgConvert((BytesMessageImpl) message);
        try {
            this.rocketmqProducer.sendOneway(rmqMessage);
        } catch (Exception ignore) { //Ignore the oneway exception.
        }
    }

    @Override
    public void send(List<Message> messages) {
        if (messages == null || messages.isEmpty()) {
            throw new OMSMessageFormatException(-1, "The messages collection is empty");
        }

        for (Message message : messages) {
            sendOneway(messages);
        }
    }

    @Override
    public Future<SendResult> sendAsync(List<Message> messages) {
        return null;
    }

    @Override
    public void sendOneway(List<Message> messages) {
        if (messages == null || messages.isEmpty()) {
            throw new OMSMessageFormatException(-1, "The messages collection is empty");
        }

        for (Message message : messages) {
            sendOneway(messages);
        }
    }

    @Override
    public void addInterceptor(ProducerInterceptor interceptor) {
    }

    @Override
    public void removeInterceptor(ProducerInterceptor interceptor) {
    }

    @Override
    public TransactionalResult prepare(Message message) {
        return null;
    }

    @Override
    public ServiceLifeState currentState() {
        return null;
    }

    @Override
    public Optional<Extension> getExtension() {
        return null;
    }

    @Override
    public QueueMetaData getQueueMetaData(String queueName) {
        return null;
    }

    @Override
    public Message createMessage(String queueName, byte[] body) {
        Message message = new BytesMessageImpl();
        message.setData(body);
        message.header().setDestination(queueName);
        return message;
    }
}
