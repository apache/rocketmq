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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.test.clientinterface.AbstractMQProducer;
import org.apache.rocketmq.test.sendresult.ResultWrapper;
import org.apache.rocketmq.test.util.RandomUtil;
import org.apache.rocketmq.test.util.TestUtil;

public class RMQAsyncSendProducer extends AbstractMQProducer {
    private static Logger logger = LoggerFactory
        .getLogger(RMQAsyncSendProducer.class);
    private String nsAddr = null;
    private DefaultMQProducer producer = null;
    private SendCallback sendCallback = null;
    private List<SendResult> successSendResult = Collections.synchronizedList(new ArrayList<SendResult>());
    private AtomicInteger exceptionMsgCount = new AtomicInteger(0);
    private int msgSize = 0;

    public RMQAsyncSendProducer(String nsAddr, String topic) {
        super(topic);
        this.nsAddr = nsAddr;
        sendCallback = new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                successSendResult.add(sendResult);
            }
            @Override
            public void onException(Throwable throwable) {
                exceptionMsgCount.getAndIncrement();
            }
        };

        create();
        start();
    }

    public int getSuccessMsgCount() {
        return successSendResult.size();
    }

    public List<SendResult> getSuccessSendResult() {
        return successSendResult;
    }

    public int getExceptionMsgCount() {
        return exceptionMsgCount.get();
    }

    private void create() {
        producer = new DefaultMQProducer();
        producer.setProducerGroup(RandomUtil.getStringByUUID());
        producer.setInstanceName(RandomUtil.getStringByUUID());

        if (nsAddr != null) {
            producer.setNamesrvAddr(nsAddr);
        }

    }

    private void start() {
        try {
            producer.start();
        } catch (MQClientException e) {
            logger.error("producer start failed!");
            e.printStackTrace();
        }
    }

    @Override
    public ResultWrapper send(Object msg, Object arg) {
        return null;
    }

    @Override
    public void shutdown() {
        producer.shutdown();
    }

    public void asyncSend(Object msg) {
        Message metaqMsg = (Message) msg;
        try {
            producer.send(metaqMsg, sendCallback);
            msgBodys.addData(new String(metaqMsg.getBody(), StandardCharsets.UTF_8));
            originMsgs.addData(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void asyncSend(int msgSize) {
        this.msgSize = msgSize;

        for (int i = 0; i < msgSize; i++) {
            Message msg = new Message(topic, RandomUtil.getStringByUUID().getBytes(StandardCharsets.UTF_8));
            this.asyncSend(msg);
        }
    }

    public void asyncSend(Object msg, MessageQueueSelector selector, Object arg) {
        Message metaqMsg = (Message) msg;
        try {
            producer.send(metaqMsg, selector, arg, sendCallback);
            msgBodys.addData(new String(metaqMsg.getBody(), StandardCharsets.UTF_8));
            originMsgs.addData(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void asyncSend(int msgSize, MessageQueueSelector selector) {
        this.msgSize = msgSize;
        for (int i = 0; i < msgSize; i++) {
            Message msg = new Message(topic, RandomUtil.getStringByUUID().getBytes(StandardCharsets.UTF_8));
            this.asyncSend(msg, selector, i);
        }
    }

    public void asyncSend(Object msg, MessageQueue mq) {
        Message metaqMsg = (Message) msg;
        try {
            producer.send(metaqMsg, mq, sendCallback);
            msgBodys.addData(new String(metaqMsg.getBody(), StandardCharsets.UTF_8));
            originMsgs.addData(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void asyncSend(int msgSize, MessageQueue mq) {
        this.msgSize = msgSize;
        for (int i = 0; i < msgSize; i++) {
            Message msg = new Message(topic, RandomUtil.getStringByUUID().getBytes(StandardCharsets.UTF_8));
            this.asyncSend(msg, mq);
        }
    }

    public void waitForResponse(int timeoutMills) {
        long startTime = System.currentTimeMillis();
        while (this.successSendResult.size() != this.msgSize) {
            if (System.currentTimeMillis() - startTime < timeoutMills) {
                TestUtil.waitForMonment(100);
            } else {
                logger.info("timeout but still not recv all response!");
                break;
            }
        }
    }

    public void sendOneWay(Object msg) {
        Message metaqMsg = (Message) msg;
        try {
            producer.sendOneway(metaqMsg);
            msgBodys.addData(new String(metaqMsg.getBody(), StandardCharsets.UTF_8));
            originMsgs.addData(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void sendOneWay(int msgSize) {
        for (int i = 0; i < msgSize; i++) {
            Message msg = new Message(topic, RandomUtil.getStringByUUID().getBytes(StandardCharsets.UTF_8));
            this.sendOneWay(msg);
        }
    }

    public void sendOneWay(Object msg, MessageQueue mq) {
        Message metaqMsg = (Message) msg;
        try {
            producer.sendOneway(metaqMsg, mq);
            msgBodys.addData(new String(metaqMsg.getBody(), StandardCharsets.UTF_8));
            originMsgs.addData(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void sendOneWay(int msgSize, MessageQueue mq) {
        for (int i = 0; i < msgSize; i++) {
            Message msg = new Message(topic, RandomUtil.getStringByUUID().getBytes(StandardCharsets.UTF_8));
            this.sendOneWay(msg, mq);
        }
    }

    public void sendOneWay(Object msg, MessageQueueSelector selector, Object arg) {
        Message metaqMsg = (Message) msg;
        try {
            producer.sendOneway(metaqMsg, selector, arg);
            msgBodys.addData(new String(metaqMsg.getBody(), StandardCharsets.UTF_8));
            originMsgs.addData(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void sendOneWay(int msgSize, MessageQueueSelector selector) {
        for (int i = 0; i < msgSize; i++) {
            Message msg = new Message(topic, RandomUtil.getStringByUUID().getBytes(StandardCharsets.UTF_8));
            this.sendOneWay(msg, selector, i);
        }
    }
}
