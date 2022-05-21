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

package org.apache.rocketmq.thinclient.impl;

public enum ClientType {
    PRODUCER,
    PUSH_CONSUMER,
    SIMPLE_CONSUMER;

    public apache.rocketmq.v2.ClientType toProtobuf() {
        if (PRODUCER.equals(this)) {
            return apache.rocketmq.v2.ClientType.PRODUCER;
        }
        if (PUSH_CONSUMER.equals(this)) {
            return apache.rocketmq.v2.ClientType.PUSH_CONSUMER;
        }
        if (SIMPLE_CONSUMER.equals(this)) {
            return apache.rocketmq.v2.ClientType.SIMPLE_CONSUMER;
        }
        return apache.rocketmq.v2.ClientType.CLIENT_TYPE_UNSPECIFIED;
    }
}
