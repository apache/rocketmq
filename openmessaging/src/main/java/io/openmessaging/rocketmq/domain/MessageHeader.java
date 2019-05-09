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
package io.openmessaging.rocketmq.domain;

import io.openmessaging.message.Header;

public class MessageHeader implements Header{

    private String destination;

    private String messageId;

    private long bornTimestamp;

    private String bornHost;

    private short priority;

    private int deliveryCount;

    private short compression;

    private short durability;

    public MessageHeader() {
    }

    @Override public Header setDestination(String destination) {
        this.destination = destination;
        return this;
    }

    @Override public Header setMessageId(String messageId) {
        this.messageId = messageId;
        return this;
    }

    @Override public Header setBornTimestamp(long bornTimestamp) {
        this.bornTimestamp = bornTimestamp;
        return this;
    }

    @Override public Header setBornHost(String bornHost) {
        this.bornHost = bornHost;
        return this;
    }

    @Override public Header setPriority(short priority) {
        this.priority = priority;
        return this;
    }

    @Override public Header setDurability(short durability) {
        this.durability = durability;
        return this;
    }

    @Override public Header setDeliveryCount(int deliveryCount) {
        this.deliveryCount = deliveryCount;
        return this;
    }

    @Override public Header setCompression(short compression) {
        this.compression = compression;
        return this;
    }

    @Override public String getDestination() {
        return this.destination;
    }

    @Override public String getMessageId() {
        return this.messageId;
    }

    @Override public long getBornTimestamp() {
        return this.bornTimestamp;
    }

    @Override public String getBornHost() {
        return this.bornHost;
    }

    @Override public short getPriority() {
        return this.priority;
    }

    @Override public short getDurability() {
        return this.durability;
    }

    @Override public int getDeliveryCount() {
        return this.deliveryCount;
    }

    @Override public short getCompression() {
        return this.compression;
    }
}
