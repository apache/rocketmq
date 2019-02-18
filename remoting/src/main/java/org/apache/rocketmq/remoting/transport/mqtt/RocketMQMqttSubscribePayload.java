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

import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttSubscribePayload;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.util.internal.StringUtil;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;
import org.apache.rocketmq.remoting.serialize.RemotingSerializable;

/**
 * Payload of {@link MqttSubscribeMessage}
 */
public final class RocketMQMqttSubscribePayload extends RemotingSerializable {

    private List<MqttTopicSubscription> topicSubscriptions;

    public RocketMQMqttSubscribePayload(List<MqttTopicSubscription> topicSubscriptions) {
        this.topicSubscriptions = Collections.unmodifiableList(topicSubscriptions);
    }

    public List<MqttTopicSubscription> getTopicSubscriptions() {
        return topicSubscriptions;
    }

    public void setTopicSubscriptions(
        List<MqttTopicSubscription> topicSubscriptions) {
        this.topicSubscriptions = topicSubscriptions;
    }

    public static RocketMQMqttSubscribePayload fromMqttSubscribePayload(MqttSubscribePayload payload) {
        return new RocketMQMqttSubscribePayload(payload.topicSubscriptions());
    }

    public MqttSubscribePayload toMqttSubscribePayload() throws UnsupportedEncodingException {
        return new MqttSubscribePayload(this.topicSubscriptions);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(StringUtil.simpleClassName(this)).append('[');
        for (int i = 0; i < topicSubscriptions.size() - 1; i++) {
            builder.append(topicSubscriptions.get(i)).append(", ");
        }
        builder.append(topicSubscriptions.get(topicSubscriptions.size() - 1));
        builder.append(']');
        return builder.toString();
    }
}
