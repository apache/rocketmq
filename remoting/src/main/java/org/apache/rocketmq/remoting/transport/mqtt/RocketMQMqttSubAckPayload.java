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

import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubAckPayload;
import io.netty.util.internal.StringUtil;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;
import org.apache.rocketmq.remoting.serialize.RemotingSerializable;

/**
 * Payload of {@link MqttSubAckMessage}
 */
public final class RocketMQMqttSubAckPayload extends RemotingSerializable {

    private List<Integer> grantedQoSLevels;

    public RocketMQMqttSubAckPayload(List<Integer> grantedQoSLevels) {
        this.grantedQoSLevels = Collections.unmodifiableList(grantedQoSLevels);
    }

    public List<Integer> getGrantedQoSLevels() {
        return grantedQoSLevels;
    }

    public void setGrantedQoSLevels(List<Integer> grantedQoSLevels) {
        this.grantedQoSLevels = grantedQoSLevels;
    }

    public static RocketMQMqttSubAckPayload fromMqttSubAckPayload(MqttSubAckPayload payload) {
        return new RocketMQMqttSubAckPayload(payload.grantedQoSLevels());
    }

    public MqttSubAckPayload toMqttSubAckPayload() throws UnsupportedEncodingException {
        return new MqttSubAckPayload(this.grantedQoSLevels);
    }

    @Override
    public String toString() {
        return StringUtil.simpleClassName(this) + '[' + "grantedQoSLevels=" + this.grantedQoSLevels + ']';
    }
}
