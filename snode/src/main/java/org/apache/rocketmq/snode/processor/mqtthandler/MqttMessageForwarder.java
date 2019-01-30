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

package org.apache.rocketmq.snode.processor.mqtthandler;

import io.netty.handler.codec.mqtt.MqttMessage;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.snode.SnodeController;

public class MqttMessageForwarder implements MessageHandler {

    private final SnodeController snodeController;
/*    private SubscriptionStore subscriptionStore;

    public MqttMessageForwarder(SubscriptionStore subscriptionStore) {
        this.subscriptionStore = subscriptionStore;
    }*/

    public MqttMessageForwarder(SnodeController snodeController) {
        this.snodeController = snodeController;
    }

    /**
     * handle PUBLISH message from client
     *
     * @param message
     * @return whether the message is handled successfully
     */
    @Override public RemotingCommand handleMessage(MqttMessage message, RemotingChannel remotingChannel) {
        return null;
    }
}
