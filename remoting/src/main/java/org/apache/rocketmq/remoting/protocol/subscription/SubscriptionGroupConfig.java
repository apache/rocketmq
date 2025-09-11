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

package org.apache.rocketmq.remoting.protocol.subscription;

import com.google.common.base.MoreObjects;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.rocketmq.common.MixAll;

public class SubscriptionGroupConfig implements Cloneable {

    private String groupName;

    private boolean consumeEnable = true;
    private boolean consumeFromMinEnable = true;
    private boolean consumeBroadcastEnable = true;
    private boolean consumeMessageOrderly = false;

    private int retryQueueNums = 1;

    private int retryMaxTimes = 16;
    private GroupRetryPolicy groupRetryPolicy = new GroupRetryPolicy();

    private long brokerId = MixAll.MASTER_ID;

    private long whichBrokerWhenConsumeSlowly = 1;

    private boolean notifyConsumerIdsChangedEnable = true;

    private int groupSysFlag = 0;

    // Only valid for push consumer
    private int consumeTimeoutMinute = 15;

    private Set<SimpleSubscriptionData> subscriptionDataSet;

    private Map<String, String> attributes = new HashMap<>();

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public boolean isConsumeEnable() {
        return consumeEnable;
    }

    public void setConsumeEnable(boolean consumeEnable) {
        this.consumeEnable = consumeEnable;
    }

    public boolean isConsumeFromMinEnable() {
        return consumeFromMinEnable;
    }

    public void setConsumeFromMinEnable(boolean consumeFromMinEnable) {
        this.consumeFromMinEnable = consumeFromMinEnable;
    }

    public boolean isConsumeBroadcastEnable() {
        return consumeBroadcastEnable;
    }

    public void setConsumeBroadcastEnable(boolean consumeBroadcastEnable) {
        this.consumeBroadcastEnable = consumeBroadcastEnable;
    }

    public boolean isConsumeMessageOrderly() {
        return consumeMessageOrderly;
    }

    public void setConsumeMessageOrderly(boolean consumeMessageOrderly) {
        this.consumeMessageOrderly = consumeMessageOrderly;
    }

    public int getRetryQueueNums() {
        return retryQueueNums;
    }

    public void setRetryQueueNums(int retryQueueNums) {
        this.retryQueueNums = retryQueueNums;
    }

    public int getRetryMaxTimes() {
        return retryMaxTimes;
    }

    public void setRetryMaxTimes(int retryMaxTimes) {
        this.retryMaxTimes = retryMaxTimes;
    }

    public GroupRetryPolicy getGroupRetryPolicy() {
        return groupRetryPolicy;
    }

    public void setGroupRetryPolicy(GroupRetryPolicy groupRetryPolicy) {
        this.groupRetryPolicy = groupRetryPolicy;
    }

    public long getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(long brokerId) {
        this.brokerId = brokerId;
    }

    public long getWhichBrokerWhenConsumeSlowly() {
        return whichBrokerWhenConsumeSlowly;
    }

    public void setWhichBrokerWhenConsumeSlowly(long whichBrokerWhenConsumeSlowly) {
        this.whichBrokerWhenConsumeSlowly = whichBrokerWhenConsumeSlowly;
    }

    public boolean isNotifyConsumerIdsChangedEnable() {
        return notifyConsumerIdsChangedEnable;
    }

    public void setNotifyConsumerIdsChangedEnable(final boolean notifyConsumerIdsChangedEnable) {
        this.notifyConsumerIdsChangedEnable = notifyConsumerIdsChangedEnable;
    }

    public int getGroupSysFlag() {
        return groupSysFlag;
    }

    public void setGroupSysFlag(int groupSysFlag) {
        this.groupSysFlag = groupSysFlag;
    }

    public int getConsumeTimeoutMinute() {
        return consumeTimeoutMinute;
    }

    public void setConsumeTimeoutMinute(int consumeTimeoutMinute) {
        this.consumeTimeoutMinute = consumeTimeoutMinute;
    }

    public Set<SimpleSubscriptionData> getSubscriptionDataSet() {
        return subscriptionDataSet;
    }

    public void setSubscriptionDataSet(Set<SimpleSubscriptionData> subscriptionDataSet) {
        this.subscriptionDataSet = subscriptionDataSet;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (brokerId ^ (brokerId >>> 32));
        result = prime * result + (consumeBroadcastEnable ? 1231 : 1237);
        result = prime * result + (consumeEnable ? 1231 : 1237);
        result = prime * result + (consumeFromMinEnable ? 1231 : 1237);
        result = prime * result + (notifyConsumerIdsChangedEnable ? 1231 : 1237);
        result = prime * result + (consumeMessageOrderly ? 1231 : 1237);
        result = prime * result + ((groupName == null) ? 0 : groupName.hashCode());
        result = prime * result + retryMaxTimes;
        result = prime * result + retryQueueNums;
        result =
            prime * result + (int) (whichBrokerWhenConsumeSlowly ^ (whichBrokerWhenConsumeSlowly >>> 32));
        result = prime * result + groupSysFlag;
        result = prime * result + consumeTimeoutMinute;
        result = prime * result + ((subscriptionDataSet == null) ? 0 : subscriptionDataSet.hashCode());
        result = prime * result + attributes.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SubscriptionGroupConfig other = (SubscriptionGroupConfig) obj;
        return new EqualsBuilder()
            .append(groupName, other.groupName)
            .append(consumeEnable, other.consumeEnable)
            .append(consumeFromMinEnable, other.consumeFromMinEnable)
            .append(consumeBroadcastEnable, other.consumeBroadcastEnable)
            .append(consumeMessageOrderly, other.consumeMessageOrderly)
            .append(retryQueueNums, other.retryQueueNums)
            .append(retryMaxTimes, other.retryMaxTimes)
            .append(whichBrokerWhenConsumeSlowly, other.whichBrokerWhenConsumeSlowly)
            .append(notifyConsumerIdsChangedEnable, other.notifyConsumerIdsChangedEnable)
            .append(groupSysFlag, other.groupSysFlag)
            .append(consumeTimeoutMinute, other.consumeTimeoutMinute)
            .append(subscriptionDataSet, other.subscriptionDataSet)
            .append(attributes, other.attributes)
            .isEquals();
    }

    @Override
    public SubscriptionGroupConfig clone() throws CloneNotSupportedException {
        SubscriptionGroupConfig clone = (SubscriptionGroupConfig) super.clone();
        if (subscriptionDataSet != null) {
            clone.setSubscriptionDataSet(new HashSet<>(subscriptionDataSet));
        }
        if (attributes != null) {
            clone.setAttributes(new HashMap<>(attributes));
        }
        return clone;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("groupName", groupName)
            .add("consumeEnable", consumeEnable)
            .add("consumeFromMinEnable", consumeFromMinEnable)
            .add("consumeBroadcastEnable", consumeBroadcastEnable)
            .add("consumeMessageOrderly", consumeMessageOrderly)
            .add("retryQueueNums", retryQueueNums)
            .add("retryMaxTimes", retryMaxTimes)
            .add("groupRetryPolicy", groupRetryPolicy)
            .add("brokerId", brokerId)
            .add("whichBrokerWhenConsumeSlowly", whichBrokerWhenConsumeSlowly)
            .add("notifyConsumerIdsChangedEnable", notifyConsumerIdsChangedEnable)
            .add("groupSysFlag", groupSysFlag)
            .add("consumeTimeoutMinute", consumeTimeoutMinute)
            .add("subscriptionDataSet", subscriptionDataSet)
            .add("attributes", attributes)
            .toString();
    }
}
