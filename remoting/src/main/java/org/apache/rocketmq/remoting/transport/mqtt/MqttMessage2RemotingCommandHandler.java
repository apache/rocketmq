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

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import java.util.List;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.transport.mqtt.dispatcher.EncodeDecodeDispatcher;
import org.apache.rocketmq.remoting.transport.mqtt.dispatcher.Message2MessageEncodeDecode;

public class MqttMessage2RemotingCommandHandler extends MessageToMessageDecoder<MqttMessage> {

    /**
     * Decode from one message to an other. This method will be called for each written message that can be handled by
     * this encoder.
     *
     * @param ctx the {@link ChannelHandlerContext} which this {@link MessageToMessageDecoder} belongs to
     * @param msg the message to decode to an other one
     * @param out the {@link List} to which decoded messages should be added
     * @throws Exception is thrown if an error occurs
     */
    @Override
    protected void decode(ChannelHandlerContext ctx, MqttMessage msg, List<Object> out)
        throws Exception {
        if (!(msg instanceof MqttMessage)) {
            return;
        }
        RemotingCommand requestCommand = null;
        Message2MessageEncodeDecode message2MessageEncodeDecode = EncodeDecodeDispatcher
            .getEncodeDecodeDispatcher().get(msg.fixedHeader().messageType());
        if (message2MessageEncodeDecode == null) {
            throw new IllegalArgumentException(
                "Unknown message type: " + msg.fixedHeader().messageType());
        }
        requestCommand = message2MessageEncodeDecode.decode(msg);
        out.add(requestCommand);
    }
}
