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
package org.apache.rocketmq.mqtt.client;

import org.apache.rocketmq.common.protocol.route.BrokerData;

public class InFlightMessage {
    private final String topic;
    private final Integer pushQos;
    private final BrokerData brokerData;
    private final byte[] body;
    private final long queueOffset;

    public InFlightMessage(String topic, Integer pushQos, byte[] body, BrokerData brokerData, long queueOffset) {
        this.topic = topic;
        this.pushQos = pushQos;
        this.body = body;
        this.brokerData = brokerData;
        this.queueOffset = queueOffset;
    }

    public String getTopic() {
        return topic;
    }

    public BrokerData getBrokerData() {
        return brokerData;
    }

    public long getQueueOffset() {
        return queueOffset;
    }

    public Integer getPushQos() {
        return pushQos;
    }

    public byte[] getBody() {
        return body;
    }
}
