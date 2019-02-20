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

package org.apache.rocketmq.snode.util;

import io.netty.handler.codec.mqtt.MqttQoS;
import java.util.UUID;
import org.apache.rocketmq.snode.constant.MqttConstant;

public class MqttUtil {

    public static String generateClientId() {
        return UUID.randomUUID().toString();
    }

    public static String getRootTopic(String topic) {
        return topic.split(MqttConstant.SUBSCRIPTION_SEPARATOR)[0];
    }

    public static int actualQos(int qos) {
        return Math.min(MqttConstant.MAX_SUPPORTED_QOS, qos);
    }

    public static boolean isQosLegal(MqttQoS qos) {
        if (!qos.equals(MqttQoS.AT_LEAST_ONCE) && !qos.equals(MqttQoS.AT_MOST_ONCE) && !qos.equals(MqttQoS.EXACTLY_ONCE)) {
            return false;
        }
        return false;
    }

    public static boolean isMatch(String topicFiter, String topic) {
        if (!topicFiter.contains(MqttConstant.SUBSCRIPTION_FLAG_PLUS) && !topicFiter.contains(MqttConstant.SUBSCRIPTION_FLAG_SHARP)) {
            return topicFiter.equals(topic);
        }
        String[] filterTopics = topicFiter.split(MqttConstant.SUBSCRIPTION_SEPARATOR);
        String[] actualTopics = topic.split(MqttConstant.SUBSCRIPTION_SEPARATOR);

        int i = 0;
        for (; i < filterTopics.length && i < actualTopics.length; i++) {
            if (MqttConstant.SUBSCRIPTION_FLAG_PLUS.equals(filterTopics[i])) {
                continue;
            }
            if (MqttConstant.SUBSCRIPTION_FLAG_SHARP.equals(filterTopics[i])) {
                return true;
            }
            if (!filterTopics[i].equals(actualTopics[i])) {
                return false;
            }
        }
        return i == actualTopics.length;
    }
}
