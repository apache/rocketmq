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

import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.test.clientinterface.AbstractMQProducer;
import org.apache.rocketmq.test.sendresult.SendResult;

public class RMQNormalProducer extends AbstractMQProducer {
    private static Logger logger = Logger.getLogger(RMQNormalProducer.class);
    private DefaultMQProducer producer = null;
    private String nsAddr = null;

    public RMQNormalProducer(String nsAddr, String topic) {
        super(topic);
        this.nsAddr = nsAddr;
        create();
        start();
    }

    public RMQNormalProducer(String nsAddr, String topic, String producerGroupName,
        String producerInstanceName) {
        super(topic);
        this.producerGroupName = producerGroupName;
        this.producerInstanceName = producerInstanceName;
        this.nsAddr = nsAddr;

        create();
        start();
    }

    public DefaultMQProducer getProducer() {
        return producer;
    }

    public void setProducer(DefaultMQProducer producer) {
        this.producer = producer;
    }

    protected void create() {
        producer = new DefaultMQProducer();
        producer.setProducerGroup(getProducerGroupName());
        producer.setInstanceName(getProducerInstanceName());

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

    public SendResult send(Object msg, Object orderKey) {
        org.apache.rocketmq.client.producer.SendResult metaqResult = null;
        Message metaqMsg = (Message) msg;
        try {
            long start = System.currentTimeMillis();
            metaqResult = producer.send(metaqMsg);
            this.msgRTs.addData(System.currentTimeMillis() - start);
            if (isDebug) {
                logger.info(metaqResult);
            }
            sendResult.setMsgId(metaqResult.getMsgId());
            sendResult.setSendResult(metaqResult.getSendStatus().equals(SendStatus.SEND_OK));
            sendResult.setBrokerIp(metaqResult.getMessageQueue().getBrokerName());
            msgBodys.addData(new String(metaqMsg.getBody()));
            originMsgs.addData(msg);
            originMsgIndex.put(new String(metaqMsg.getBody()), metaqResult);
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

    public SendResult sendMQ(Message msg, MessageQueue mq) {
        org.apache.rocketmq.client.producer.SendResult metaqResult = null;
        try {
            long start = System.currentTimeMillis();
            metaqResult = producer.send(msg, mq);
            this.msgRTs.addData(System.currentTimeMillis() - start);
            if (isDebug) {
                logger.info(metaqResult);
            }
            sendResult.setMsgId(metaqResult.getMsgId());
            sendResult.setSendResult(metaqResult.getSendStatus().equals(SendStatus.SEND_OK));
            sendResult.setBrokerIp(metaqResult.getMessageQueue().getBrokerName());
            msgBodys.addData(new String(msg.getBody()));
            originMsgs.addData(msg);
            originMsgIndex.put(new String(msg.getBody()), metaqResult);
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
