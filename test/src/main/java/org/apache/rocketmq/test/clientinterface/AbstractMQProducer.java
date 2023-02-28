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

package org.apache.rocketmq.test.clientinterface;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.sendresult.ResultWrapper;
import org.apache.rocketmq.test.util.RandomUtil;
import org.apache.rocketmq.test.util.TestUtil;

public abstract class AbstractMQProducer extends MQCollector implements MQProducer {
    protected String topic = null;

    protected ResultWrapper sendResult = new ResultWrapper();
    protected boolean startSuccess = false;
    protected String producerGroupName = null;
    protected String producerInstanceName = null;
    protected boolean isDebug = false;


    public AbstractMQProducer(String topic) {
        super();
        producerGroupName = RandomUtil.getStringByUUID();
        producerInstanceName = RandomUtil.getStringByUUID();
        this.topic = topic;

    }

    public AbstractMQProducer(String topic, String originMsgCollector, String msgBodyCollector) {
        super(originMsgCollector, msgBodyCollector);
        producerGroupName = RandomUtil.getStringByUUID();
        producerInstanceName = RandomUtil.getStringByUUID();
        this.topic = topic;
    }

    public boolean isStartSuccess() {
        return startSuccess;
    }

    public void setStartSuccess(boolean startSuccess) {
        this.startSuccess = startSuccess;
    }

    public String getProducerInstanceName() {
        return producerInstanceName;
    }

    public void setProducerInstanceName(String producerInstanceName) {
        this.producerInstanceName = producerInstanceName;
    }

    public String getProducerGroupName() {
        return producerGroupName;
    }

    public void setProducerGroupName(String producerGroupName) {
        this.producerGroupName = producerGroupName;
    }

    public void setDebug() {
        isDebug = true;
    }

    public void setDebug(boolean isDebug) {
        this.isDebug = isDebug;
    }

    public void setRun() {
        isDebug = false;
    }

    public List<MessageQueue> getMessageQueue() {
        return null;
    }

    private Object getMessage() {
        return this.getMessageByTag(null);
    }

    public Object getMessageByTag(String tag) {
        Object objMsg = null;
        if (this instanceof RMQNormalProducer) {
            org.apache.rocketmq.common.message.Message msg = new org.apache.rocketmq.common.message.Message(
                topic, (RandomUtil.getStringByUUID() + "." + new Date()).getBytes(StandardCharsets.UTF_8));
            objMsg = msg;
            if (tag != null) {
                msg.setTags(tag);
            }
        }
        return objMsg;
    }

    public void send() {
        Object msg = getMessage();
        send(msg, null);
    }

    public void send(Object msg) {
        send(msg, null);
    }

    public void send(long msgNum) {
        for (int i = 0; i < msgNum; i++) {
            this.send();
        }
    }

    public void send(long msgNum, int intervalMills) {
        for (int i = 0; i < msgNum; i++) {
            this.send();
            TestUtil.waitForMoment(intervalMills);
        }
    }

    public void send(String tag, int msgSize) {
        for (int i = 0; i < msgSize; i++) {
            Object msg = getMessageByTag(tag);
            send(msg, null);
        }
    }

    public void send(String tag, int msgSize, int intervalMills) {
        for (int i = 0; i < msgSize; i++) {
            Object msg = getMessageByTag(tag);
            send(msg, null);
            TestUtil.waitForMoment(intervalMills);
        }
    }

    public void send(List<Object> msgs) {
        for (Object msg : msgs) {
            this.send(msg, null);
        }
    }
}
