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
package org.apache.rocketmq.remoting.protocol.header;

import com.google.common.base.MoreObjects;
import io.netty.buffer.ByteBuf;
import java.util.HashMap;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.common.resource.RocketMQResource;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.FastCodesHeader;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.rpc.TopicQueueRequestHeader;

@RocketMQAction(value = RequestCode.SEND_MESSAGE, action = Action.PUB)
public class SendMessageRequestHeader extends TopicQueueRequestHeader implements FastCodesHeader {
    @CFNotNull
    private String producerGroup;
    @CFNotNull
    @RocketMQResource(ResourceType.TOPIC)
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
    private Boolean unitMode;
    @CFNullable
    private Boolean batch;
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

    public void setBornTimestamp(long bornTimestamp) {
        this.bornTimestamp = bornTimestamp;
    }

    public void setBornTimestamp(Long bornTimestamp) {
        this.bornTimestamp = bornTimestamp != null ? bornTimestamp : 0L;
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
        if (null == reconsumeTimes) {
            return 0;
        }
        return reconsumeTimes;
    }

    public void setReconsumeTimes(Integer reconsumeTimes) {
        this.reconsumeTimes = reconsumeTimes;
    }

    public boolean isUnitMode() {
        if (null == unitMode) {
            return false;
        }
        return unitMode;
    }

    public void setUnitMode(Boolean isUnitMode) {
        this.unitMode = isUnitMode;
    }

    public Integer getMaxReconsumeTimes() {
        return maxReconsumeTimes;
    }

    public void setMaxReconsumeTimes(final Integer maxReconsumeTimes) {
        this.maxReconsumeTimes = maxReconsumeTimes;
    }

    public boolean isBatch() {
        if (null == batch) {
            return false;
        }
        return batch;
    }

    public void setBatch(Boolean batch) {
        this.batch = batch;
    }

    public static SendMessageRequestHeader parseRequestHeader(RemotingCommand request) throws RemotingCommandException {
        return request.decodeCommandCustomHeader(SendMessageRequestHeader.class);
    }

    @Override
    public void encode(ByteBuf out) {
        writeIfNotNull(out, "a", producerGroup);
        writeIfNotNull(out, "b", topic);
        writeIfNotNull(out, "c", defaultTopic);
        writeIfNotNull(out, "d", defaultTopicQueueNums);
        writeIfNotNull(out, "e", queueId);
        writeIfNotNull(out, "f", sysFlag);
        writeIfNotNull(out, "g", bornTimestamp);
        writeIfNotNull(out, "h", flag);
        writeIfNotNull(out, "i", properties);
        writeIfNotNull(out, "j", reconsumeTimes);
        writeIfNotNull(out, "k", unitMode);
        writeIfNotNull(out, "l", maxReconsumeTimes);
        writeIfNotNull(out, "m", batch);
        writeIfNotNull(out, "n", getBname());
    }

    @Override
    public void decode(HashMap<String, String> fields) throws RemotingCommandException {
        String str = fields.get("a");
        if (str == null) {
            str = getAndCheckNotNull(fields, "producerGroup");
        }
        if (str != null) {
            this.producerGroup = str;
        }

        str = fields.get("b");
        if (str == null) {
            str = getAndCheckNotNull(fields, "topic");
        }
        if (str != null) {
            this.topic = str;
        }

        str = fields.get("c");
        if (str == null) {
            str = getAndCheckNotNull(fields, "defaultTopic");
        }
        if (str != null) {
            this.defaultTopic = str;
        }

        str = fields.get("d");
        if (str == null) {
            str = getAndCheckNotNull(fields, "defaultTopicQueueNums");
        }
        if (str != null) {
            this.defaultTopicQueueNums = Integer.parseInt(str);
        }

        str = fields.get("e");
        if (str == null) {
            str = getAndCheckNotNull(fields, "queueId");
        }
        if (str != null) {
            this.queueId = Integer.parseInt(str);
        }

        str = fields.get("f");
        if (str == null) {
            str = getAndCheckNotNull(fields, "sysFlag");
        }
        if (str != null) {
            this.sysFlag = Integer.parseInt(str);
        }

        str = fields.get("g");
        if (str == null) {
            str = getAndCheckNotNull(fields, "bornTimestamp");
        }
        if (str != null) {
            this.bornTimestamp = Long.parseLong(str);
        }

        str = fields.get("h");
        if (str == null) {
            str = getAndCheckNotNull(fields, "flag");
        }
        if (str != null) {
            this.flag = Integer.parseInt(str);
        }

        str = fields.get("i");
        if (str == null) {
            str = fields.get("properties");
        }
        if (str != null) {
            this.properties = str;
        }

        str = fields.get("j");
        if (str == null) {
            str = fields.get("reconsumeTimes");
        }
        if (str != null) {
            this.reconsumeTimes = Integer.parseInt(str);
        }

        str = fields.get("k");
        if (str == null) {
            str = fields.get("unitMode");
        }
        if (str != null) {
            this.unitMode = Boolean.parseBoolean(str);
        }

        str = fields.get("l");
        if (str == null) {
            str = fields.get("maxReconsumeTimes");
        }
        if (str != null) {
            this.maxReconsumeTimes = Integer.parseInt(str);
        }

        str = fields.get("m");
        if (str == null) {
            str = fields.get("batch");
        }
        if (str != null) {
            this.batch = Boolean.parseBoolean(str);
        }

        str = fields.get("n");
        if (str == null) {
            str = fields.get("brokerName");
        }
        if (str != null) {
            this.setBrokerName(str);
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
