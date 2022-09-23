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
 * $Id: PullMessageRequestHeader.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.header;

import com.google.common.base.MoreObjects;
import java.util.HashMap;
import org.apache.rocketmq.common.rpc.TopicQueueRequestHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.FastCodesHeader;

import io.netty.buffer.ByteBuf;

public class PullMessageRequestHeader extends TopicQueueRequestHeader implements FastCodesHeader {
    @CFNotNull
    private String consumerGroup;
    @CFNotNull
    private String topic;
    @CFNotNull
    private Integer queueId;
    @CFNotNull
    private Long queueOffset;
    @CFNotNull
    private Integer maxMsgNums;
    @CFNotNull
    private Integer sysFlag;
    @CFNotNull
    private Long commitOffset;
    @CFNotNull
    private Long suspendTimeoutMillis;
    @CFNullable
    private String subscription;
    @CFNotNull
    private Long subVersion;
    private String expressionType;

    @CFNullable
    private Integer maxMsgBytes;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    @Override
    public void encode(ByteBuf out) {
        writeIfNotNull(out, "consumerGroup", consumerGroup);
        writeIfNotNull(out, "topic", topic);
        writeIfNotNull(out, "queueId", queueId);
        writeIfNotNull(out, "queueOffset", queueOffset);
        writeIfNotNull(out, "maxMsgNums", maxMsgNums);
        writeIfNotNull(out, "sysFlag", sysFlag);
        writeIfNotNull(out, "commitOffset", commitOffset);
        writeIfNotNull(out, "suspendTimeoutMillis", suspendTimeoutMillis);
        writeIfNotNull(out, "subscription", subscription);
        writeIfNotNull(out, "subVersion", subVersion);
        writeIfNotNull(out, "expressionType", expressionType);
        writeIfNotNull(out, "maxMsgBytes", maxMsgBytes);
        writeIfNotNull(out, "lo", lo);
        writeIfNotNull(out, "ns", ns);
        writeIfNotNull(out, "nsd", nsd);
        writeIfNotNull(out, "bname", bname);
        writeIfNotNull(out, "oway", oway);
    }

    @Override
    public void decode(HashMap<String, String> fields) throws RemotingCommandException {
        String str = getAndCheckNotNull(fields, "consumerGroup");
        if (str != null) {
            this.consumerGroup = str;
        }

        str = getAndCheckNotNull(fields, "topic");
        if (str != null) {
            this.topic = str;
        }

        str = getAndCheckNotNull(fields, "queueId");
        if (str != null) {
            this.queueId = Integer.parseInt(str);
        }

        str = getAndCheckNotNull(fields, "queueOffset");
        if (str != null) {
            this.queueOffset = Long.parseLong(str);
        }

        str = getAndCheckNotNull(fields, "maxMsgNums");
        if (str != null) {
            this.maxMsgNums = Integer.parseInt(str);
        }

        str = getAndCheckNotNull(fields, "sysFlag");
        if (str != null) {
            this.sysFlag = Integer.parseInt(str);
        }

        str = getAndCheckNotNull(fields, "commitOffset");
        if (str != null) {
            this.commitOffset = Long.parseLong(str);
        }

        str = getAndCheckNotNull(fields, "suspendTimeoutMillis");
        if (str != null) {
            this.suspendTimeoutMillis = Long.parseLong(str);
        }

        str = fields.get("subscription");
        if (str != null) {
            this.subscription = str;
        }

        str = getAndCheckNotNull(fields, "subVersion");
        if (str != null) {
            this.subVersion = Long.parseLong(str);
        }

        str = fields.get("expressionType");
        if (str != null) {
            this.expressionType = str;
        }

        str = fields.get("maxMsgBytes");
        if (str != null) {
            this.maxMsgBytes = Integer.parseInt(str);
        }

        str = fields.get("lo");
        if (str != null) {
            this.lo = Boolean.parseBoolean(str);
        }

        str = fields.get("ns");
        if (str != null) {
            this.ns = str;
        }

        str = fields.get("nsd");
        if (str != null) {
            this.nsd = Boolean.parseBoolean(str);
        }

        str = fields.get("bname");
        if (str != null) {
            this.bname = str;
        }

        str = fields.get("oway");
        if (str != null) {
            this.oway = Boolean.parseBoolean(str);
        }
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Override
    public Integer getQueueId() {
        return queueId;
    }

    @Override
    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }

    public Long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(Long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public Integer getMaxMsgNums() {
        return maxMsgNums;
    }

    public void setMaxMsgNums(Integer maxMsgNums) {
        this.maxMsgNums = maxMsgNums;
    }

    public Integer getSysFlag() {
        return sysFlag;
    }

    public void setSysFlag(Integer sysFlag) {
        this.sysFlag = sysFlag;
    }

    public Long getCommitOffset() {
        return commitOffset;
    }

    public void setCommitOffset(Long commitOffset) {
        this.commitOffset = commitOffset;
    }

    public Long getSuspendTimeoutMillis() {
        return suspendTimeoutMillis;
    }

    public void setSuspendTimeoutMillis(Long suspendTimeoutMillis) {
        this.suspendTimeoutMillis = suspendTimeoutMillis;
    }

    public String getSubscription() {
        return subscription;
    }

    public void setSubscription(String subscription) {
        this.subscription = subscription;
    }

    public Long getSubVersion() {
        return subVersion;
    }

    public void setSubVersion(Long subVersion) {
        this.subVersion = subVersion;
    }

    public String getExpressionType() {
        return expressionType;
    }

    public void setExpressionType(String expressionType) {
        this.expressionType = expressionType;
    }

    public Integer getMaxMsgBytes() {
        return maxMsgBytes;
    }

    public void setMaxMsgBytes(Integer maxMsgBytes) {
        this.maxMsgBytes = maxMsgBytes;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("consumerGroup", consumerGroup)
            .add("topic", topic)
            .add("queueId", queueId)
            .add("queueOffset", queueOffset)
            .add("maxMsgBytes", maxMsgBytes)
            .add("maxMsgNums", maxMsgNums)
            .add("sysFlag", sysFlag)
            .add("commitOffset", commitOffset)
            .add("suspendTimeoutMillis", suspendTimeoutMillis)
            .add("subscription", subscription)
            .add("subVersion", subVersion)
            .add("expressionType", expressionType)
            .toString();
    }
}
