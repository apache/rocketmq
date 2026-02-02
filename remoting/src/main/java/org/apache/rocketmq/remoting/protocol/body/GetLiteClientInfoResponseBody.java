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

import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.Set;

public class GetLiteClientInfoResponseBody extends RemotingSerializable {

    private String parentTopic;
    private String group;
    private String clientId;
    private long lastAccessTime;
    private long lastConsumeTime;
    private int liteTopicCount;
    private Set<String> liteTopicSet;

    public String getParentTopic() {
        return parentTopic;
    }

    public void setParentTopic(String parentTopic) {
        this.parentTopic = parentTopic;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    public long getLastConsumeTime() {
        return lastConsumeTime;
    }

    public void setLastConsumeTime(long lastConsumeTime) {
        this.lastConsumeTime = lastConsumeTime;
    }

    public int getLiteTopicCount() {
        return liteTopicCount;
    }

    public void setLiteTopicCount(int liteTopicCount) {
        this.liteTopicCount = liteTopicCount;
    }

    public Set<String> getLiteTopicSet() {
        return liteTopicSet;
    }

    public void setLiteTopicSet(Set<String> liteTopicSet) {
        this.liteTopicSet = liteTopicSet;
    }
}
