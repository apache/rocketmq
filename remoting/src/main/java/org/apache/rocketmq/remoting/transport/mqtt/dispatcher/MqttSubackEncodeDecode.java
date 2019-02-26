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

package org.apache.rocketmq.remoting.transport.mqtt.dispatcher;

import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubAckPayload;
import java.io.UnsupportedEncodingException;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.transport.mqtt.MqttHeader;
import org.apache.rocketmq.remoting.util.MqttEncodeDecodeUtil;

public class MqttSubackEncodeDecode implements Message2MessageEncodeDecode {

    @Override
    public RemotingCommand decode(MqttMessage mqttMessage) {
        return null;
    }

    @Override
    public MqttMessage encode(RemotingCommand remotingCommand) throws RemotingCommandException, UnsupportedEncodingException {
        MqttHeader mqttHeader = (MqttHeader) remotingCommand.readCustomHeader();
        return new MqttSubAckMessage(
            new MqttFixedHeader(MqttMessageType.SUBACK, mqttHeader.isDup(),
                MqttQoS.valueOf(mqttHeader.getQosLevel()), mqttHeader.isRetain(),
                mqttHeader.getRemainingLength()),
            MqttMessageIdVariableHeader.from(mqttHeader.getMessageId()), (MqttSubAckPayload) MqttEncodeDecodeUtil.decode(remotingCommand.getBody(),MqttSubAckPayload.class));
    }
}
