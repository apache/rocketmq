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
package org.apache.rocketmq.common.message;

import com.google.common.base.Joiner;
import org.apache.rocketmq.common.BrokerAddrInfo;

import java.io.Serializable;
import java.util.Objects;

public class MessageQueueInfo implements Serializable {
    private static final long serialVersionUID = -8861235963083685405L;

    private final String topic;
    private final String brokerName;
    private final int queueId;
    private final BrokerAddrInfo leader;
    private final BrokerAddrInfo[] followers;

    public MessageQueueInfo(String topic, String brokerName, int queueId, BrokerAddrInfo leader, BrokerAddrInfo[] followers) {
        this.topic = topic;
        this.brokerName = brokerName;
        this.queueId = queueId;
        this.leader = leader;
        this.followers = followers;
    }

    public String getTopic() {
        return topic;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public int getQueueId() {
        return queueId;
    }

    public BrokerAddrInfo getLeader() {
        return leader;
    }

    public BrokerAddrInfo[] getFollowers() {
        return followers;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((brokerName == null) ? 0 : brokerName.hashCode());
        result = prime * result + queueId;
        result = prime * result + ((topic == null) ? 0 : topic.hashCode());
        result = prime * result + ((leader == null) ? 0 : leader.hashCode());
        result = prime * result + ((followers == null) ? 0 : followers.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        MessageQueueInfo other = (MessageQueueInfo) obj;
        if (brokerName == null) {
            if (other.brokerName != null)
                return false;
        } else if (!brokerName.equals(other.brokerName))
            return false;
        if (queueId != other.queueId)
            return false;
        if (topic == null) {
            if (other.topic != null)
                return false;
        } else if (!topic.equals(other.topic))
            return false;

        return Objects.equals(leader, other.leader) &&
                Objects.equals(followers, other.followers);
    }

    @Override
    public String toString() {
        return "MessageQueueInfo [topic=" + topic + ", brokerName=" + brokerName + ", queueId=" + queueId +
                ", leader: " + leader + ", followers: " + Joiner.on(", ").join(followers) + "]";
    }
}
