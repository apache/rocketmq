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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.client.rmq;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.test.clientinterface.AbstractMQProducer;
import org.apache.rocketmq.test.sendresult.ResultWrapper;

public class RMQNormalProducer extends AbstractMQProducer {
    private static Logger logger = LoggerFactory.getLogger(RMQNormalProducer.class);
    private DefaultMQProducer producer = null;
    private String nsAddr = null;

    public RMQNormalProducer(String nsAddr, String topic) {
        this(nsAddr, topic, false);
    }

    public RMQNormalProducer(String nsAddr, String topic, boolean useTLS) {
        super(topic);
        this.nsAddr = nsAddr;
        create(useTLS);
        start();
    }

    public RMQNormalProducer(String nsAddr, String topic, String producerGroupName,
        String producerInstanceName) {
        this(nsAddr, topic, producerGroupName, producerInstanceName, false);
    }

    public RMQNormalProducer(String nsAddr, String topic, String producerGroupName,
        String producerInstanceName, boolean useTLS) {
        super(topic);
        this.producerGroupName = producerGroupName;
        this.producerInstanceName = producerInstanceName;
        this.nsAddr = nsAddr;

        create(useTLS);
        start();
    }

    public DefaultMQProducer getProducer() {
        return producer;
    }

    public void setProducer(DefaultMQProducer producer) {
        this.producer = producer;
    }

    protected void create(boolean useTLS) {
        producer = new DefaultMQProducer();
        producer.setProducerGroup(getProducerGroupName());
        producer.setInstanceName(getProducerInstanceName());
        producer.setUseTLS(useTLS);
        producer.setPollNameServerInterval(100);

        if (nsAddr != null) {
            producer.setNamesrvAddr(nsAddr);
        }
    }


    public void start() {
        try {
            producer.start();
            super.setStartSuccess(true);
        } catch (MQClientException e) {
            super.setStartSuccess(false);
            logger.error("producer start failed!");
            e.printStackTrace();
        }
    }

    public ResultWrapper send(Object msg, Object orderKey) {
        org.apache.rocketmq.client.producer.SendResult internalSendResult = null;
        Message message = (Message) msg;
        try {
            long start = System.currentTimeMillis();
            internalSendResult = producer.send(message);
            this.msgRTs.addData(System.currentTimeMillis() - start);
            if (isDebug) {
                logger.info("SendResult: {}", internalSendResult);
            }
            sendResult.setMsgId(internalSendResult.getMsgId());
            sendResult.setSendResult(internalSendResult.getSendStatus().equals(SendStatus.SEND_OK));
            sendResult.setBrokerIp(internalSendResult.getMessageQueue().getBrokerName());
            msgBodys.addData(new String(message.getBody(), StandardCharsets.UTF_8));
            originMsgs.addData(msg);
            originMsgIndex.put(new String(message.getBody(), StandardCharsets.UTF_8), internalSendResult);
        } catch (Exception e) {
            if (isDebug) {
                e.printStackTrace();
            }

            sendResult.setSendResult(false);
            sendResult.setSendException(e);
            errorMsgs.addData(msg);
        }

        return sendResult;
    }

    public void send(Map<MessageQueue, List<Object>> msgs) {
        for (MessageQueue mq : msgs.keySet()) {
            send(msgs.get(mq), mq);
        }
    }

    public void send(List<Object> msgs, MessageQueue mq) {
        for (Object msg : msgs) {
            sendMQ((Message) msg, mq);
        }
    }

    public void send(int num, MessageQueue mq) {
        for (int i = 0; i < num; i++) {
            sendMQ((Message) getMessageByTag(null), mq);
        }
    }

    public ResultWrapper sendMQ(Message msg, MessageQueue mq) {
        org.apache.rocketmq.client.producer.SendResult internalSendResult = null;
        try {
            long start = System.currentTimeMillis();
            internalSendResult = producer.send(msg, mq);
            this.msgRTs.addData(System.currentTimeMillis() - start);
            if (isDebug) {
                logger.info("SendResult: {}", internalSendResult);
            }
            sendResult.setMsgId(internalSendResult.getMsgId());
            sendResult.setSendResult(internalSendResult.getSendStatus().equals(SendStatus.SEND_OK));
            sendResult.setBrokerIp(internalSendResult.getMessageQueue().getBrokerName());
            msgBodys.addData(new String(msg.getBody(), StandardCharsets.UTF_8));
            originMsgs.addData(msg);
            originMsgIndex.put(new String(msg.getBody(), StandardCharsets.UTF_8), internalSendResult);
        } catch (Exception e) {
            if (isDebug) {
                e.printStackTrace();
            }

            sendResult.setSendResult(false);
            sendResult.setSendException(e);
            errorMsgs.addData(msg);
        }

        return sendResult;
    }

    public void shutdown() {
        producer.shutdown();
    }

    @Override
    public List<MessageQueue> getMessageQueue() {
        List<MessageQueue> mqs = null;
        try {
            mqs = producer.fetchPublishMessageQueues(topic);
        } catch (MQClientException e) {
            e.printStackTrace();
        }
        return mqs;
    }
}
