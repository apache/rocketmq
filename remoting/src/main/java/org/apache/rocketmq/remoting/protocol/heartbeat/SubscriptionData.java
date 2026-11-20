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
 * $Id: SubscriptionData.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.remoting.protocol.heartbeat;

import com.alibaba.fastjson2.annotation.JSONField;
import org.apache.rocketmq.common.filter.ExpressionType;

import java.util.HashSet;
import java.util.Set;

/**
 * A consumer's subscription to a single topic,
 *   - exchanged between client and broker via heartbeat
 *   - consumed during message filtering.
 *
 * <p>Carries:
 *   - the tag/SQL92 filter expression ({@link #subString})
 *   - its precomputed {@link #codeSet} (tag {@code hashCode()}s)
 * used by the broker for O(1) tag matching
 *   against the per-message {@code tagsCode} stored in the ConsumeQueue.
 *
 * <p>The {@link #subVersion} timestamp lets the broker detect
 *   and propagate subscription changes.
 */
public class SubscriptionData implements Comparable<SubscriptionData> {
    public final static String SUB_ALL = "*";
    private boolean classFilterMode = false;
    private String topic;
    private String subString;
    /**
     * Verbatim tags subscribed by the consumer.
     * populated alongside {@link #codeSet} by {@code FilterAPI#buildSubscriptionData}
     * when it splits the {@link #subString} on {@code "||"}.
     *
     * <p>Unlike {@link #codeSet}, this holds the original tag strings
     * and is used only for display, logging ({@link #toString}) and serialization;
     * it is not consulted during broker-side filtering.
     */
    private Set<String> tagsSet = new HashSet<>();
    /**
     * Precomputed {@code hashCode()} of each tag in {@link #tagsSet},
     * used by the broker for O(1) tag matching.
     *
     * <p>The broker compares this set against the per-message {@code tagsCode}
     * (also produced by {@code tagsString2tagsCode}, i.e. {@code String#hashCode})
     * stored in the ConsumeQueue entry, so a subscription matches a message when
     * {@code codeSet.contains(message.tagsCode)} holds. Because these are 32-bit
     * hashes, a collision can yield a false-positive match on the broker, which is
     * why the consumer still performs exact tag filtering after delivery.
     *
     * <p>Empty (or only {@link #SUB_ALL} subscription) means "match everything":
     * the broker short-circuits tag filtering entirely.
     */
    private Set<Integer> codeSet = new HashSet<>();
    private long subVersion = System.currentTimeMillis();
    private String expressionType = ExpressionType.TAG;

    @JSONField(serialize = false)
    private String filterClassSource;

    public SubscriptionData() {

    }

    public SubscriptionData(String topic, String subString) {
        super();
        this.topic = topic;
        this.subString = subString;
    }

    public String getFilterClassSource() {
        return filterClassSource;
    }

    public void setFilterClassSource(String filterClassSource) {
        this.filterClassSource = filterClassSource;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getSubString() {
        return subString;
    }

    public void setSubString(String subString) {
        this.subString = subString;
    }

    public Set<String> getTagsSet() {
        return tagsSet;
    }

    public void setTagsSet(Set<String> tagsSet) {
        this.tagsSet = tagsSet;
    }

    public long getSubVersion() {
        return subVersion;
    }

    public void setSubVersion(long subVersion) {
        this.subVersion = subVersion;
    }

    public Set<Integer> getCodeSet() {
        return codeSet;
    }

    public void setCodeSet(Set<Integer> codeSet) {
        this.codeSet = codeSet;
    }

    public boolean isClassFilterMode() {
        return classFilterMode;
    }

    public void setClassFilterMode(boolean classFilterMode) {
        this.classFilterMode = classFilterMode;
    }

    public String getExpressionType() {
        return expressionType;
    }

    public void setExpressionType(String expressionType) {
        this.expressionType = expressionType;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (classFilterMode ? 1231 : 1237);
        result = prime * result + ((codeSet == null) ? 0 : codeSet.hashCode());
        result = prime * result + ((subString == null) ? 0 : subString.hashCode());
        result = prime * result + ((tagsSet == null) ? 0 : tagsSet.hashCode());
        result = prime * result + ((topic == null) ? 0 : topic.hashCode());
        result = prime * result + ((expressionType == null) ? 0 : expressionType.hashCode());
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
        SubscriptionData other = (SubscriptionData) obj;
        if (classFilterMode != other.classFilterMode)
            return false;
        if (codeSet == null) {
            if (other.codeSet != null)
                return false;
        } else if (!codeSet.equals(other.codeSet))
            return false;
        if (subString == null) {
            if (other.subString != null)
                return false;
        } else if (!subString.equals(other.subString))
            return false;
        if (subVersion != other.subVersion)
            return false;
        if (tagsSet == null) {
            if (other.tagsSet != null)
                return false;
        } else if (!tagsSet.equals(other.tagsSet))
            return false;
        if (topic == null) {
            if (other.topic != null)
                return false;
        } else if (!topic.equals(other.topic))
            return false;
        if (expressionType == null) {
            if (other.expressionType != null)
                return false;
        } else if (!expressionType.equals(other.expressionType))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "SubscriptionData [classFilterMode=" + classFilterMode + ", topic=" + topic + ", subString="
            + subString + ", tagsSet=" + tagsSet + ", codeSet=" + codeSet + ", subVersion=" + subVersion
            + ", expressionType=" + expressionType + "]";
    }

    @Override
    public int compareTo(SubscriptionData other) {
        String thisValue = this.topic + "@" + this.subString;
        String otherValue = other.topic + "@" + other.subString;
        return thisValue.compareTo(otherValue);
    }
}
