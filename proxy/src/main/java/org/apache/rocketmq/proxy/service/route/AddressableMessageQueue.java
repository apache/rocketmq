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
package org.apache.rocketmq.proxy.service.route;

import com.google.common.base.MoreObjects;
import org.apache.rocketmq.common.message.MessageQueue;

public class AddressableMessageQueue extends MessageQueue {
    private final String brokerAddr;

    public AddressableMessageQueue(MessageQueue messageQueue, String brokerAddr) {
        super(messageQueue);
        this.brokerAddr = brokerAddr;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public MessageQueue getMessageQueue() {
        return new MessageQueue(getTopic(), getBrokerName(), getQueueId());
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AddressableMessageQueue)) {
            return false;
        }
        return super.equals(o);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("messageQueue", super.toString())
            .add("brokerAddr", brokerAddr)
            .toString();
    }
}
