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

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LiteSubscription {
    private String group;
    private String topic;
    private final Set<String> liteTopicSet = ConcurrentHashMap.newKeySet();
    /**
     * Wildcard patterns for a wildcard group in pattern mode. Empty means the group is either a
     * non-wildcard subscription or a legacy wildcard group (receives all lite-topics under the
     * parent topic). When non-empty, {@link #liteTopicSet} holds the expanded (matched) lmqNames.
     * Not serialized over the wire; rebuilt on client reconnect, same as {@link #liteTopicSet}.
     */
    private final Set<String> wildcardPatterns = ConcurrentHashMap.newKeySet();
    private volatile long updateTime = System.currentTimeMillis();

    public boolean addLiteTopic(String liteTopic) {
        updateTime();
        return this.liteTopicSet.add(liteTopic);
    }

    public void addLiteTopic(Collection<String> set) {
        updateTime();
        this.liteTopicSet.addAll(set);
    }

    public boolean removeLiteTopic(String liteTopic) {
        updateTime();
        return this.liteTopicSet.remove(liteTopic);
    }

    public void removeLiteTopic(Collection<String> set) {
        updateTime();
        this.liteTopicSet.removeAll(set);
    }

    public String getGroup() {
        return group;
    }

    public LiteSubscription setGroup(String group) {
        this.group = group;
        return this;
    }

    public String getTopic() {
        return topic;
    }

    public LiteSubscription setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public Set<String> getLiteTopicSet() {
        return liteTopicSet;
    }

    public LiteSubscription setLiteTopicSet(Set<String> liteTopicSet) {
        this.liteTopicSet.addAll(liteTopicSet);
        return this;
    }

    public Set<String> getWildcardPatterns() {
        return wildcardPatterns;
    }

    /**
     * Full replace of the wildcard patterns. Existing patterns not in the given collection are
     * removed and the new ones are added.
     */
    public LiteSubscription setWildcardPatterns(Collection<String> patterns) {
        updateTime();
        this.wildcardPatterns.clear();
        if (patterns != null) {
            this.wildcardPatterns.addAll(patterns);
        }
        return this;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }

    private void updateTime() {
        this.updateTime = System.currentTimeMillis();
    }

    @Override
    public String toString() {
        return "LiteSubscription{" +
            "group='" + group + '\'' +
            ", topic='" + topic + '\'' +
            ", liteTopicSet=" + liteTopicSet +
            ", wildcardPatterns=" + wildcardPatterns +
            ", updateTime=" + updateTime +
            '}';
    }
}
