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
package com.alibaba.rocketmq.broker.mqtrace;

import java.util.Properties;


public class SendMessageContext {
    private String producerGroup;
    private String topic;
    private String msgId;
    private String originMsgId;
    private Integer queueId;
    private Long queueOffset;
    private String brokerAddr;
    private String bornHost;
    private int bodyLength;
    private int code;
    private String errorMsg;
    private String msgProps;
    private Object mqTraceContext;
    private Properties extProps;


    public String getProducerGroup() {
        return producerGroup;
    }


    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }


    public String getTopic() {
        return topic;
    }


    public void setTopic(String topic) {
        this.topic = topic;
    }


    public String getMsgId() {
        return msgId;
    }


    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }


    public String getOriginMsgId() {
        return originMsgId;
    }


    public void setOriginMsgId(String originMsgId) {
        this.originMsgId = originMsgId;
    }


    public Integer getQueueId() {
        return queueId;
    }


    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }


    public Long getQueueOffset() {
        return queueOffset;
    }


    public void setQueueOffset(Long queueOffset) {
        this.queueOffset = queueOffset;
    }


    public String getBrokerAddr() {
        return brokerAddr;
    }


    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = brokerAddr;
    }


    public String getBornHost() {
        return bornHost;
    }


    public void setBornHost(String bornHost) {
        this.bornHost = bornHost;
    }


    public int getBodyLength() {
        return bodyLength;
    }


    public void setBodyLength(int bodyLength) {
        this.bodyLength = bodyLength;
    }


    public int getCode() {
        return code;
    }


    public void setCode(int code) {
        this.code = code;
    }


    public String getErrorMsg() {
        return errorMsg;
    }


    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }


    public String getMsgProps() {
        return msgProps;
    }


    public void setMsgProps(String msgProps) {
        this.msgProps = msgProps;
    }


    public Object getMqTraceContext() {
        return mqTraceContext;
    }


    public void setMqTraceContext(Object mqTraceContext) {
        this.mqTraceContext = mqTraceContext;
    }


    public Properties getExtProps() {
        return extProps;
    }


    public void setExtProps(Properties extProps) {
        this.extProps = extProps;
    }
}
