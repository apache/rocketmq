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

package org.apache.rocketmq.common.lite;

import java.util.Set;

public class LiteSubscriptionDTO {
    private LiteSubscriptionAction action;
    private String clientId;
    private String group;
    private String topic;
    private Set<String> liteTopicSet;
    private OffsetOption offsetOption;
    private long version;

    public LiteSubscriptionAction getAction() {
        return action;
    }

    public LiteSubscriptionDTO setAction(LiteSubscriptionAction action) {
        this.action = action;
        return this;
    }

    public String getClientId() {
        return clientId;
    }

    public LiteSubscriptionDTO setClientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    public String getGroup() {
        return group;
    }

    public LiteSubscriptionDTO setGroup(String group) {
        this.group = group;
        return this;
    }

    public String getTopic() {
        return topic;
    }

    public LiteSubscriptionDTO setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public Set<String> getLiteTopicSet() {
        return liteTopicSet;
    }

    public LiteSubscriptionDTO setLiteTopicSet(Set<String> liteTopicSet) {
        this.liteTopicSet = liteTopicSet;
        return this;
    }

    public OffsetOption getOffsetOption() {
        return offsetOption;
    }

    public void setOffsetOption(OffsetOption offsetOption) {
        this.offsetOption = offsetOption;
    }

    public long getVersion() {
        return version;
    }

    public LiteSubscriptionDTO setVersion(long version) {
        this.version = version;
        return this;
    }

    @Override
    public String toString() {
        return "LiteSubscriptionDTO{" + "action=" + action +
            ", clientId='" + clientId + '\'' +
            ", group='" + group + '\'' +
            ", topic='" + topic + '\'' +
            ", liteTopicSet=" + liteTopicSet +
            ", offsetOption=" + offsetOption +
            ", version=" + version +
            '}';
    }
}
