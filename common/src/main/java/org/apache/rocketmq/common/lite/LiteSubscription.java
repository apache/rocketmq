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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LiteSubscription {
    private String group;
    private String topic;
    private final Set<String> lmqSet = ConcurrentHashMap.newKeySet();
    private volatile long updateTime = System.currentTimeMillis();

    public LiteSubscription touch() {
        this.updateTime = System.currentTimeMillis();
        return this;
    }

    public boolean addLmq(String lmqName) {
        return this.lmqSet.add(lmqName);
    }

    public boolean removeLmq(String lmqName) {
        return this.lmqSet.remove(lmqName);
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

    public Set<String> getLmqSet() {
        return lmqSet;
    }

    public LiteSubscription setLmqSet(Set<String> lmqSet) {
        this.lmqSet.clear();
        this.lmqSet.addAll(lmqSet);
        return this;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }

    public static Set<String> removals(Set<String> current, Set<String> target) {
        Set<String> safeTarget = target == null ? Collections.emptySet() : target;
        Set<String> result = new HashSet<>();
        for (String item : current) {
            if (!safeTarget.contains(item)) {
                result.add(item);
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return "LiteSubscription{" +
            "group='" + group + '\'' +
            ", topic='" + topic + '\'' +
            ", lmqSet=" + lmqSet +
            ", updateTime=" + updateTime +
            '}';
    }

}
