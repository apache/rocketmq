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

package org.apache.rocketmq.remoting.protocol.header;

import com.google.common.base.MoreObjects;
import io.netty.buffer.ByteBuf;
import java.util.HashMap;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.FastCodesHeader;

/**
 * Use short variable name to speed up FastJson deserialization process.
 */
public class SendMessageRequestHeaderV2 implements CommandCustomHeader, FastCodesHeader {
    @CFNotNull
    private String producerGroup; // producerGroup;
    @CFNotNull
    private String topic; // topic;
    @CFNotNull
    private String defaultTopic; // defaultTopic;
    @CFNotNull
    private Integer defaultTopicQueueNums; // defaultTopicQueueNums;
    @CFNotNull
    private Integer queueId; // queueId;
    @CFNotNull
    private Integer sysFlag; // sysFlag;
    @CFNotNull
    private Long bornTimestamp; // bornTimestamp;
    @CFNotNull
    private Integer flag; // flag;
    @CFNullable
    private String properties; // properties;
    @CFNullable
    private Integer reconsumeTimes; // reconsumeTimes;
    @CFNullable
    private boolean unitMode; // unitMode = false;

    private Integer consumeRetryTimes; // consumeRetryTimes

    @CFNullable
    private boolean batch; //batch
    @CFNullable
    private String brokerName; // brokerName

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
        v1.setBatch(v2.batch);
        v1.setBname(v2.brokerName);
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
        v2.batch = v1.isBatch();
        v2.brokerName = v1.getBname();
        return v2;
    }

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    @Override
    public void encode(ByteBuf out) {
        writeIfNotNull(out, "producerGroup", producerGroup);
        writeIfNotNull(out, "topic", topic);
        writeIfNotNull(out, "defaultTopic", defaultTopic);
        writeIfNotNull(out, "defaultTopicQueueNums", defaultTopicQueueNums);
        writeIfNotNull(out, "queueId", queueId);
        writeIfNotNull(out, "sysFlag", sysFlag);
        writeIfNotNull(out, "bornTimestamp", bornTimestamp);
        writeIfNotNull(out, "flag", flag);
        writeIfNotNull(out, "properties", properties);
        writeIfNotNull(out, "reconsumeTimes", reconsumeTimes);
        writeIfNotNull(out, "unitMode", unitMode);
        writeIfNotNull(out, "consumeRetryTimes", consumeRetryTimes);
        writeIfNotNull(out, "batch", batch);
        writeIfNotNull(out, "brokerName", brokerName);
    }

    @Override
    public void decode(HashMap<String, String> fields) throws RemotingCommandException {

        String str = getAndCheckNotNull(fields, "producerGroup");
        if (str != null) {
            producerGroup = str;
        }

        str = getAndCheckNotNull(fields, "topic");
        if (str != null) {
            topic = str;
        }

        str = getAndCheckNotNull(fields, "defaultTopic");
        if (str != null) {
            defaultTopic = str;
        }

        str = getAndCheckNotNull(fields, "defaultTopicQueueNums");
        if (str != null) {
            defaultTopicQueueNums = Integer.parseInt(str);
        }

        str = getAndCheckNotNull(fields, "queueId");
        if (str != null) {
            queueId = Integer.parseInt(str);
        }

        str = getAndCheckNotNull(fields, "sysFlag");
        if (str != null) {
            sysFlag = Integer.parseInt(str);
        }

        str = getAndCheckNotNull(fields, "bornTimestamp");
        if (str != null) {
            bornTimestamp = Long.parseLong(str);
        }

        str = getAndCheckNotNull(fields, "flag");
        if (str != null) {
            flag = Integer.parseInt(str);
        }

        str = fields.get("properties");
        if (str != null) {
            properties = str;
        }

        str = fields.get("reconsumeTimes");
        if (str != null) {
            reconsumeTimes = Integer.parseInt(str);
        }

        str = fields.get("unitMode");
        if (str != null) {
            unitMode = Boolean.parseBoolean(str);
        }

        str = fields.get("consumeRetryTimes");
        if (str != null) {
            consumeRetryTimes = Integer.parseInt(str);
        }

        str = fields.get("batch");
        if (str != null) {
            batch = Boolean.parseBoolean(str);
        }

        str = fields.get("brokerName");
        if (str != null) {
            brokerName = str;
        }
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

    public boolean isBatch() {
        return batch;
    }

    public void setBatch(boolean batch) {
        this.batch = batch;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("producerGroup", producerGroup)
            .add("topic", topic)
            .add("defaultTopic", defaultTopic)
            .add("defaultTopicQueueNums", defaultTopicQueueNums)
            .add("queueId", queueId)
            .add("sysFlag", sysFlag)
            .add("bornTimestamp", bornTimestamp)
            .add("flag", flag)
            .add("properties", properties)
            .add("reconsumeTimes", reconsumeTimes)
            .add("unitMode", unitMode)
            .add("consumeRetryTimes", consumeRetryTimes)
            .add("batch", batch)
            .add("brokerName", brokerName)
            .toString();
    }
}
