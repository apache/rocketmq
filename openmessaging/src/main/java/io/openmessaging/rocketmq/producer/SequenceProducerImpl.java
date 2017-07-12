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
import io.openmessaging.SequenceProducer;
import io.openmessaging.rocketmq.utils.OMSUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;

public class SequenceProducerImpl extends AbstractOMSProducer implements SequenceProducer {

    private BlockingQueue<Message> msgCacheQueue;

    public SequenceProducerImpl(final KeyValue properties) {
        super(properties);
        this.msgCacheQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public KeyValue properties() {
        return properties;
    }

    @Override
    public void send(final Message message) {
        checkMessageType(message);
        org.apache.rocketmq.common.message.Message rmqMessage = OMSUtil.msgConvert((BytesMessage) message);
        try {
            Validators.checkMessage(rmqMessage, this.rocketmqProducer);
        } catch (MQClientException e) {
            throw checkProducerException(rmqMessage.getTopic(), message.headers().getString(MessageHeader.MESSAGE_ID), e);
        }
        msgCacheQueue.add(message);
    }

    @Override
    public void send(final Message message, final KeyValue properties) {
        send(message);
    }

    @Override
    public synchronized void commit() {
        List<Message> messages = new ArrayList<>();
        msgCacheQueue.drainTo(messages);

        List<org.apache.rocketmq.common.message.Message> rmqMessages = new ArrayList<>();

        for (Message message : messages) {
            rmqMessages.add(OMSUtil.msgConvert((BytesMessage) message));
        }

        if (rmqMessages.size() == 0) {
            return;
        }

        try {
            SendResult sendResult = this.rocketmqProducer.send(rmqMessages);
            String[] msgIdArray = sendResult.getMsgId().split(",");
            for (int i = 0; i < messages.size(); i++) {
                Message message = messages.get(i);
                message.headers().put(MessageHeader.MESSAGE_ID, msgIdArray[i]);
            }
        } catch (Exception e) {
            throw checkProducerException("", "", e);
        }
    }

    @Override
    public synchronized void rollback() {
        msgCacheQueue.clear();
    }
}
