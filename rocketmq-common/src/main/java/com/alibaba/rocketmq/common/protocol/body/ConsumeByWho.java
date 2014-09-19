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
package com.alibaba.rocketmq.common.protocol.body;

import java.util.HashSet;

import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-8-10
 */
public class ConsumeByWho extends RemotingSerializable {
    private HashSet<String> consumedGroup = new HashSet<String>();
    private HashSet<String> notConsumedGroup = new HashSet<String>();
    private String topic;
    private int queueId;
    private long offset;


    public HashSet<String> getConsumedGroup() {
        return consumedGroup;
    }


    public void setConsumedGroup(HashSet<String> consumedGroup) {
        this.consumedGroup = consumedGroup;
    }


    public HashSet<String> getNotConsumedGroup() {
        return notConsumedGroup;
    }


    public void setNotConsumedGroup(HashSet<String> notConsumedGroup) {
        this.notConsumedGroup = notConsumedGroup;
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


    public long getOffset() {
        return offset;
    }


    public void setOffset(long offset) {
        this.offset = offset;
    }
}
