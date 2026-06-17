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

package org.apache.rocketmq.remoting.protocol.body;

import com.alibaba.fastjson2.annotation.JSONField;

public class ChangeInvisibleTimeRequestEntry {
    private String consumerGroup;
    private String topic;
    private int queueId;
    private String extraInfo;
    private long offset;
    private long invisibleTime;
    private String liteTopic;
    private boolean suspend;

    // broker only
    private transient long popTime;
    private transient long oldInvisibleTime;
    private transient long changedPopTime;
    private transient long changedInvisibleTime;

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public String getExtraInfo() {
        return extraInfo;
    }

    public void setExtraInfo(String extraInfo) {
        this.extraInfo = extraInfo;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getInvisibleTime() {
        return invisibleTime;
    }

    public void setInvisibleTime(long invisibleTime) {
        this.invisibleTime = invisibleTime;
    }

    public String getLiteTopic() {
        return liteTopic;
    }

    public void setLiteTopic(String liteTopic) {
        this.liteTopic = liteTopic;
    }

    public boolean isSuspend() {
        return suspend;
    }

    public void setSuspend(boolean suspend) {
        this.suspend = suspend;
    }

    @JSONField(serialize = false, deserialize = false)
    public long getPopTime() {
        return popTime;
    }

    @JSONField(serialize = false, deserialize = false)
    public void setPopTime(long popTime) {
        this.popTime = popTime;
    }

    @JSONField(serialize = false, deserialize = false)
    public long getOldInvisibleTime() {
        return oldInvisibleTime;
    }

    @JSONField(serialize = false, deserialize = false)
    public void setOldInvisibleTime(long oldInvisibleTime) {
        this.oldInvisibleTime = oldInvisibleTime;
    }

    @JSONField(serialize = false, deserialize = false)
    public long getChangedPopTime() {
        return changedPopTime;
    }

    @JSONField(serialize = false, deserialize = false)
    public void setChangedPopTime(long changedPopTime) {
        this.changedPopTime = changedPopTime;
    }

    @JSONField(serialize = false, deserialize = false)
    public long getChangedInvisibleTime() {
        return changedInvisibleTime;
    }

    @JSONField(serialize = false, deserialize = false)
    public void setChangedInvisibleTime(long changedInvisibleTime) {
        this.changedInvisibleTime = changedInvisibleTime;
    }
}
