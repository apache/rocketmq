/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.client.hook;

import java.util.Map;

import com.alibaba.rocketmq.client.impl.CommunicationMode;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageQueue;


public class SendMessageContext {
    private String producerGroup;
    private Message message;
    private MessageQueue mq;
    private String brokerAddr;
    private String bornHost;
    private CommunicationMode communicationMode;
    private SendResult sendResult;
    private Exception exception;
    private Object mqTraceContext;
    private Map<String, String> props;


    public String getProducerGroup() {
        return producerGroup;
    }


    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }


    public Message getMessage() {
        return message;
    }


    public void setMessage(Message message) {
        this.message = message;
    }


    public MessageQueue getMq() {
        return mq;
    }


    public void setMq(MessageQueue mq) {
        this.mq = mq;
    }


    public String getBrokerAddr() {
        return brokerAddr;
    }


    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = brokerAddr;
    }


    public CommunicationMode getCommunicationMode() {
        return communicationMode;
    }


    public void setCommunicationMode(CommunicationMode communicationMode) {
        this.communicationMode = communicationMode;
    }


    public SendResult getSendResult() {
        return sendResult;
    }


    public void setSendResult(SendResult sendResult) {
        this.sendResult = sendResult;
    }


    public Exception getException() {
        return exception;
    }


    public void setException(Exception exception) {
        this.exception = exception;
    }


    public Object getMqTraceContext() {
        return mqTraceContext;
    }


    public void setMqTraceContext(Object mqTraceContext) {
        this.mqTraceContext = mqTraceContext;
    }


    public Map<String, String> getProps() {
        return props;
    }


    public void setProps(Map<String, String> props) {
        this.props = props;
    }


    public String getBornHost() {
        return bornHost;
    }


    public void setBornHost(String bornHost) {
        this.bornHost = bornHost;
    }
}
