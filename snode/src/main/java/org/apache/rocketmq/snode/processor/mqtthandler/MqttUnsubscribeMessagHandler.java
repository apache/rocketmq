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

/**
 * handle the UNSUBSCRIBE message from the client
 * <ol>
 *     <li>extract topic filters to be un-subscribed</li>
 *     <li>get the topics matching with the topic filters</li>
 *     <li>verify the authorization of the client to the </li>
 *     <li>remove subscription from the SubscriptionStore</li>
 * </ol>
 */
public class MqttUnsubscribeMessagHandler implements MessageHandler {

/*    private SubscriptionStore subscriptionStore;

    public MqttUnsubscribeMessagHandler(SubscriptionStore subscriptionStore) {
        this.subscriptionStore = subscriptionStore;
    }*/
    private final SnodeController snodeController;

    public MqttUnsubscribeMessagHandler(SnodeController snodeController) {
        this.snodeController = snodeController;
    }
    @Override
    public RemotingCommand handleMessage(MqttMessage message, RemotingChannel remotingChannel) {
        return null;
    }
}
