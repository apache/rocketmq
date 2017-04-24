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

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.Producer;
import io.openmessaging.Promise;
import io.openmessaging.PropertyKeys;
import io.openmessaging.SendResult;
import io.openmessaging.exception.OMSRuntimeException;
import io.openmessaging.rocketmq.promise.DefaultPromise;
import io.openmessaging.rocketmq.utils.OMSUtil;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendStatus;

import static io.openmessaging.rocketmq.utils.OMSUtil.msgConvert;

public class ProducerImpl extends AbstractOMSProducer implements Producer {

    public ProducerImpl(final KeyValue properties) {
        super(properties);
    }

    @Override
    public KeyValue properties() {
        return properties;
    }

    @Override
    public SendResult send(final Message message) {
        return send(message, this.rocketmqProducer.getSendMsgTimeout());
    }

    @Override
    public SendResult send(final Message message, final KeyValue properties) {
        long timeout = properties.containsKey(PropertyKeys.OPERATION_TIMEOUT)
            ? properties.getInt(PropertyKeys.OPERATION_TIMEOUT) : this.rocketmqProducer.getSendMsgTimeout();
        return send(message, timeout);
    }

    private SendResult send(final Message message, long timeout) {
        checkMessageType(message);
        org.apache.rocketmq.common.message.Message rmqMessage = msgConvert((BytesMessage) message);
        try {
            org.apache.rocketmq.client.producer.SendResult rmqResult = this.rocketmqProducer.send(rmqMessage, timeout);
            if (!rmqResult.getSendStatus().equals(SendStatus.SEND_OK)) {
                log.error(String.format("Send message to RocketMQ failed, %s", message));
                throw new OMSRuntimeException("-1", "Send message to RocketMQ broker failed.");
            }
            message.headers().put(MessageHeader.MESSAGE_ID, rmqResult.getMsgId());
            return OMSUtil.sendResultConvert(rmqResult);
        } catch (Exception e) {
            log.error(String.format("Send message to RocketMQ failed, %s", message), e);
            throw checkProducerException(rmqMessage.getTopic(), message.headers().getString(MessageHeader.MESSAGE_ID), e);
        }
    }

    @Override
    public Promise<SendResult> sendAsync(final Message message) {
        return sendAsync(message, this.rocketmqProducer.getSendMsgTimeout());
    }

    @Override
    public Promise<SendResult> sendAsync(final Message message, final KeyValue properties) {
        long timeout = properties.containsKey(PropertyKeys.OPERATION_TIMEOUT)
            ? properties.getInt(PropertyKeys.OPERATION_TIMEOUT) : this.rocketmqProducer.getSendMsgTimeout();
        return sendAsync(message, timeout);
    }

    private Promise<SendResult> sendAsync(final Message message, long timeout) {
        checkMessageType(message);
        org.apache.rocketmq.common.message.Message rmqMessage = msgConvert((BytesMessage) message);
        final Promise<SendResult> promise = new DefaultPromise<>();
        try {
            this.rocketmqProducer.send(rmqMessage, new SendCallback() {
                @Override
                public void onSuccess(final org.apache.rocketmq.client.producer.SendResult rmqResult) {
                    message.headers().put(MessageHeader.MESSAGE_ID, rmqResult.getMsgId());
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
        org.apache.rocketmq.common.message.Message rmqMessage = msgConvert((BytesMessage) message);
        try {
            this.rocketmqProducer.sendOneway(rmqMessage);
        } catch (Exception ignore) { //Ignore the oneway exception.
        }
    }

    @Override
    public void sendOneway(final Message message, final KeyValue properties) {
        sendOneway(message);
    }
}
