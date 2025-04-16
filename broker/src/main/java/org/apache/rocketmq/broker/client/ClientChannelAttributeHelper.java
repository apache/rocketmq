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
package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ClientChannelAttributeHelper {
    private static final AttributeKey<String> ATTR_CG = AttributeKey.valueOf("CHANNEL_CONSUMER_GROUP");
    private static final AttributeKey<String> ATTR_PG = AttributeKey.valueOf("CHANNEL_PRODUCER_GROUP");
    private static final String SEPARATOR = "|";

    public static void addProducerGroup(Channel channel, String group) {
        addGroup(channel, group, ATTR_PG);
    }

    public static void addConsumerGroup(Channel channel, String group) {
        addGroup(channel, group, ATTR_CG);
    }

    public static List<String> getProducerGroups(Channel channel) {
        return getGroups(channel, ATTR_PG);
    }

    public static List<String> getConsumerGroups(Channel channel) {
        return getGroups(channel, ATTR_CG);
    }

    private static void addGroup(Channel channel, String group, AttributeKey<String> key) {
        if (null == channel || !channel.isActive()) {  // no side effect if check active status.
            return;
        }
        if (null == group || group.length() == 0 || null == key) {
            return;
        }
        String groups = channel.attr(key).get();
        if (null == groups) {
            channel.attr(key).set(group + SEPARATOR);
        } else {
            if (groups.contains(SEPARATOR + group + SEPARATOR)) {
                return;
            } else {
                channel.attr(key).compareAndSet(groups, groups + group + SEPARATOR);
            }
        }
    }

    private static List<String> getGroups(Channel channel, AttributeKey<String> key) {
        if (null == channel) {
            return Collections.emptyList();
        }
        if (null == key) {
            return Collections.emptyList();
        }
        String groups = channel.attr(key).get();
        return null == groups ? Collections.<String>emptyList() : Arrays.asList(groups.split("\\|"));
    }

}