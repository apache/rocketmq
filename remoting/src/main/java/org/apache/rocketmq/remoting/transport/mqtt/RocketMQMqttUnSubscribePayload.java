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

package org.apache.rocketmq.remoting.transport.mqtt;

import io.netty.handler.codec.mqtt.MqttUnsubscribePayload;
import io.netty.util.internal.StringUtil;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;
import org.apache.rocketmq.remoting.serialize.RemotingSerializable;

/**
 * Payload of the {@link io.netty.handler.codec.mqtt.MqttUnsubscribeMessage}
 */
public class RocketMQMqttUnSubscribePayload extends RemotingSerializable {
    private List<String> topics;

    public RocketMQMqttUnSubscribePayload(List<String> topics) {
        this.topics = topics;
    }

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = Collections.unmodifiableList(topics);
    }

    public static RocketMQMqttUnSubscribePayload fromMqttUnSubscribePayload(MqttUnsubscribePayload payload) {
        return new RocketMQMqttUnSubscribePayload(payload.topics());
    }

    public MqttUnsubscribePayload toMqttUnsubscribePayload() throws UnsupportedEncodingException {
        return new MqttUnsubscribePayload(this.topics);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(StringUtil.simpleClassName(this)).append('[');
        for (int i = 0; i < topics.size() - 1; i++) {
            builder.append("topicName = ").append(topics.get(i)).append(", ");
        }
        builder.append("topicName = ").append(topics.get(topics.size() - 1))
            .append(']');
        return builder.toString();
    }
}
