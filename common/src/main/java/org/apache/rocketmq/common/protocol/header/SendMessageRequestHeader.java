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

/**
 * $Id: SendMessageRequestHeader.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.header;

import com.google.common.base.MoreObjects;
import java.util.HashMap;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.rpc.TopicQueueRequestHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class SendMessageRequestHeader extends TopicQueueRequestHeader {
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
    private boolean unitMode = false;
    @CFNullable
    private boolean batch = false;
    private Integer maxReconsumeTimes;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
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

    @Override
    public Integer getQueueId() {
        return queueId;
    }

    @Override
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

    public void setUnitMode(boolean isUnitMode) {
        this.unitMode = isUnitMode;
    }

    public Integer getMaxReconsumeTimes() {
        return maxReconsumeTimes;
    }

    public void setMaxReconsumeTimes(final Integer maxReconsumeTimes) {
        this.maxReconsumeTimes = maxReconsumeTimes;
    }

    public boolean isBatch() {
        return batch;
    }

    public void setBatch(boolean batch) {
        this.batch = batch;
    }

    public static SendMessageRequestHeader parseRequestHeader(RemotingCommand request) throws RemotingCommandException {
        SendMessageRequestHeaderV2 requestHeaderV2 = null;
        SendMessageRequestHeader requestHeader = null;
        switch (request.getCode()) {
            case RequestCode.SEND_BATCH_MESSAGE:
            case RequestCode.SEND_MESSAGE_V2:
                requestHeaderV2 =
                    (SendMessageRequestHeaderV2) request
                        .decodeCommandCustomHeader(SendMessageRequestHeaderV2.class);
            case RequestCode.SEND_MESSAGE:
                if (null == requestHeaderV2) {
                    requestHeader =
                        (SendMessageRequestHeader) request
                            .decodeCommandCustomHeader(SendMessageRequestHeader.class);
                } else {
                    requestHeader = SendMessageRequestHeaderV2.createSendMessageRequestHeaderV1(requestHeaderV2);
                }
            default:
                break;
        }
        return requestHeader;
    }

    public static SendMessageRequestHeaderV2 decodeSendMessageHeaderV2(RemotingCommand request)
        throws RemotingCommandException {
        SendMessageRequestHeaderV2 r = new SendMessageRequestHeaderV2();
        HashMap<String, String> fields = request.getExtFields();
        if (fields == null) {
            throw new RemotingCommandException("the ext fields is null");
        }

        String s = fields.get("a");
        checkNotNull(s, "the custom field <a> is null");
        r.setA(s);

        s = fields.get("b");
        checkNotNull(s, "the custom field <b> is null");
        r.setB(s);

        s = fields.get("c");
        checkNotNull(s, "the custom field <c> is null");
        r.setC(s);

        s = fields.get("d");
        checkNotNull(s, "the custom field <d> is null");
        r.setD(Integer.parseInt(s));

        s = fields.get("e");
        checkNotNull(s, "the custom field <e> is null");
        r.setE(Integer.parseInt(s));

        s = fields.get("f");
        checkNotNull(s, "the custom field <f> is null");
        r.setF(Integer.parseInt(s));

        s = fields.get("g");
        checkNotNull(s, "the custom field <g> is null");
        r.setG(Long.parseLong(s));

        s = fields.get("h");
        checkNotNull(s, "the custom field <h> is null");
        r.setH(Integer.parseInt(s));

        s = fields.get("i");
        if (s != null) {
            r.setI(s);
        }

        s = fields.get("j");
        if (s != null) {
            r.setJ(Integer.parseInt(s));
        }

        s = fields.get("k");
        if (s != null) {
            r.setK(Boolean.parseBoolean(s));
        }

        s = fields.get("l");
        if (s != null) {
            r.setL(Integer.parseInt(s));
        }

        s = fields.get("m");
        if (s != null) {
            r.setM(Boolean.parseBoolean(s));
        }
        return r;
    }

    private static void checkNotNull(String s, String msg) throws RemotingCommandException {
        if (s == null) {
            throw new RemotingCommandException(msg);
        }
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
            .add("batch", batch)
            .add("maxReconsumeTimes", maxReconsumeTimes)
            .toString();
    }
}
