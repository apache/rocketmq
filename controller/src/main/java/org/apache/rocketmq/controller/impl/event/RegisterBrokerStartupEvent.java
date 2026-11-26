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
package org.apache.rocketmq.controller.impl.event;

public class RegisterBrokerStartupEvent implements EventMessage {
    private final String brokerName;
    private final Long brokerId;
    private final String brokerAddress;
    private final long brokerStartupGeneration;
    private final String brokerStartupId;

    public RegisterBrokerStartupEvent(String brokerName, Long brokerId, String brokerAddress,
        long brokerStartupGeneration, String brokerStartupId) {
        this.brokerName = brokerName;
        this.brokerId = brokerId;
        this.brokerAddress = brokerAddress;
        this.brokerStartupGeneration = brokerStartupGeneration;
        this.brokerStartupId = brokerStartupId;
    }

    @Override
    public EventType getEventType() {
        return EventType.REGISTER_BROKER_STARTUP_EVENT;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public Long getBrokerId() {
        return brokerId;
    }

    public String getBrokerAddress() {
        return brokerAddress;
    }

    public long getBrokerStartupGeneration() {
        return brokerStartupGeneration;
    }

    public String getBrokerStartupId() {
        return brokerStartupId;
    }
}
