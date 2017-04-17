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
package io.openmessaging.rocketmq.consumer;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;

public class PullConsumerImpl implements PullConsumer {
    public PullConsumerImpl(final KeyValue properties) {

    }

    @Override
    public KeyValue properties() {
        return null;
    }

    @Override
    public Message poll() {
        return null;
    }

    @Override
    public Message poll(final KeyValue properties) {
        return null;
    }

    @Override
    public void ack(final String messageId) {

    }

    @Override
    public void ack(final String messageId, final KeyValue properties) {

    }

    @Override
    public void startup() {

    }

    @Override
    public void shutdown() {

    }
}
