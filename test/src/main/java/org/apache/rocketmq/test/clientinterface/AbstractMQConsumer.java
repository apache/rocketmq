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

import org.apache.rocketmq.test.listener.AbstractListener;

public abstract class AbstractMQConsumer implements MQConsumer {
    protected AbstractListener listener = null;
    protected String nsAddr = null;
    protected String topic = null;
    protected String subExpression = null;
    protected String consumerGroup = null;
    protected boolean isDebug = false;

    public AbstractMQConsumer() {
    }

    public AbstractMQConsumer(String nsAddr, String topic, String subExpression,
        String consumerGroup, AbstractListener listener) {
        this.topic = topic;
        this.subExpression = subExpression;
        this.consumerGroup = consumerGroup;
        this.listener = listener;
        this.nsAddr = nsAddr;
    }

    public AbstractMQConsumer(String topic, String subExpression) {
        this.topic = topic;
        this.subExpression = subExpression;
    }

    public void setDebug() {
        if (listener != null) {
            listener.setDebug(true);
        }

        isDebug = true;
    }

    public void setDebug(boolean isDebug) {
        if (listener != null) {
            listener.setDebug(isDebug);
        }

        this.isDebug = isDebug;
    }

    public void setSubscription(String topic, String subExpression) {
        this.topic = topic;
        this.subExpression = subExpression;
    }

    public AbstractListener getListener() {
        return listener;
    }

    public void setListener(AbstractListener listner) {
        this.listener = listner;
    }

    public String getNsAddr() {
        return nsAddr;
    }

    public void setNsAddr(String nsAddr) {
        this.nsAddr = nsAddr;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getSubExpression() {
        return subExpression;
    }

    public void setSubExpression(String subExpression) {
        this.subExpression = subExpression;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public void clearMsg() {
        listener.clearMsg();
    }

}
