/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain producerGroup copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.rocketmq.common.protocol.header;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.annotation.CFNullable;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


/**
 * @author shijia.wxr
 */
public class SendMessageRequestHeaderV2 implements CommandCustomHeader {
    @CFNotNull
    private String producerGroup;
    @CFNotNull
    private String topic;
    @CFNotNull
    private String defaultTopic;
    @CFNotNull
    private Integer defaultTopicQueueNums;
    @CFNotNull
    private Integer queueId;
    @CFNotNull
    private Integer sysFlag;
    @CFNotNull
    private Long bornTimestamp;
    @CFNotNull
    private Integer flag;
    @CFNullable
    private String properties;
    @CFNullable
    private Integer reconsumeTimes;
    @CFNullable
    private boolean unitMode;

    private Integer consumeRetryTimes;

    public static SendMessageRequestHeader createSendMessageRequestHeaderV1(final SendMessageRequestHeaderV2 v2) {
        SendMessageRequestHeader v1 = new SendMessageRequestHeader();
        v1.setProducerGroup(v2.producerGroup);
        v1.setTopic(v2.topic);
        v1.setDefaultTopic(v2.defaultTopic);
        v1.setDefaultTopicQueueNums(v2.defaultTopicQueueNums);
        v1.setQueueId(v2.queueId);
        v1.setSysFlag(v2.sysFlag);
        v1.setBornTimestamp(v2.bornTimestamp);
        v1.setFlag(v2.flag);
        v1.setProperties(v2.properties);
        v1.setReconsumeTimes(v2.reconsumeTimes);
        v1.setUnitMode(v2.unitMode);
        v1.setMaxReconsumeTimes(v2.consumeRetryTimes);
        return v1;
    }

    public static SendMessageRequestHeaderV2 createSendMessageRequestHeaderV2(final SendMessageRequestHeader v1) {
        SendMessageRequestHeaderV2 v2 = new SendMessageRequestHeaderV2();
        v2.producerGroup = v1.getProducerGroup();
        v2.topic = v1.getTopic();
        v2.defaultTopic = v1.getDefaultTopic();
        v2.defaultTopicQueueNums = v1.getDefaultTopicQueueNums();
        v2.queueId = v1.getQueueId();
        v2.sysFlag = v1.getSysFlag();
        v2.bornTimestamp = v1.getBornTimestamp();
        v2.flag = v1.getFlag();
        v2.properties = v1.getProperties();
        v2.reconsumeTimes = v1.getReconsumeTimes();
        v2.unitMode = v1.isUnitMode();
        v2.consumeRetryTimes = v1.getMaxReconsumeTimes();
        return v2;
    }

    @Override
    public void checkFields() throws RemotingCommandException {
    }

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


    public String getDefaultTopic() {
        return defaultTopic;
    }


    public void setDefaultTopic(String defaultTopic) {
        this.defaultTopic = defaultTopic;
    }


    public Integer getDefaultTopicQueueNums() {
        return defaultTopicQueueNums;
    }


    public void setDefaultTopicQueueNums(Integer defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }


    public Integer getQueueId() {
        return queueId;
    }


    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }


    public Integer getSysFlag() {
        return sysFlag;
    }


    public void setSysFlag(Integer sysFlag) {
        this.sysFlag = sysFlag;
    }


    public Long getBornTimestamp() {
        return bornTimestamp;
    }


    public void setBornTimestamp(Long bornTimestamp) {
        this.bornTimestamp = bornTimestamp;
    }


    public Integer getFlag() {
        return flag;
    }


    public void setFlag(Integer flag) {
        this.flag = flag;
    }


    public String getProperties() {
        return properties;
    }


    public void setProperties(String properties) {
        this.properties = properties;
    }


    public Integer getReconsumeTimes() {
        return reconsumeTimes;
    }


    public void setReconsumeTimes(Integer reconsumeTimes) {
        this.reconsumeTimes = reconsumeTimes;
    }


    public boolean isUnitMode() {
        return unitMode;
    }


    public void setUnitMode(boolean unitMode) {
        this.unitMode = unitMode;
    }


    public Integer getConsumeRetryTimes() {
        return consumeRetryTimes;
    }


    public void setConsumeRetryTimes(final Integer consumeRetryTimes) {
        this.consumeRetryTimes = consumeRetryTimes;
    }
}
