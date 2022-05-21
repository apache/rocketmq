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

package org.apache.rocketmq.thinclient.message;

public enum MessageType {
    NORMAL,
    FIFO,
    DELAY,
    TRANSACTION;

    public static MessageType fromProtobuf(apache.rocketmq.v2.MessageType messageType) {
        switch (messageType) {
            case NORMAL:
                return MessageType.NORMAL;
            case FIFO:
                return MessageType.FIFO;
            case DELAY:
                return MessageType.DELAY;
            case TRANSACTION:
                return MessageType.TRANSACTION;
            case MESSAGE_TYPE_UNSPECIFIED:
            default:
                throw new IllegalArgumentException("Message type is not specified");
        }
    }

    public static apache.rocketmq.v2.MessageType toProtobuf(MessageType messageType) {
        switch (messageType) {
            case NORMAL:
                return apache.rocketmq.v2.MessageType.NORMAL;
            case FIFO:
                return apache.rocketmq.v2.MessageType.FIFO;
            case DELAY:
                return apache.rocketmq.v2.MessageType.DELAY;
            case TRANSACTION:
                return apache.rocketmq.v2.MessageType.TRANSACTION;
            default:
                return apache.rocketmq.v2.MessageType.MESSAGE_TYPE_UNSPECIFIED;
        }
    }
}
