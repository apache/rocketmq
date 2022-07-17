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

public class UpdateBrokerInfoEvent implements EventMessage {

    private final String brokerName;
    private final String brokerIdentity;
    private final String brokerAddress;

    public UpdateBrokerInfoEvent(String brokerName, String brokerIdentity, String brokerAddress) {
        this.brokerName = brokerName;
        this.brokerIdentity = brokerIdentity;
        this.brokerAddress = brokerAddress;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public String getBrokerIdentity() {
        return brokerIdentity;
    }

    public String getBrokerAddress() {
        return brokerAddress;
    }

    @Override public EventType getEventType() {
        return EventType.UPDATE_BROKER_INFO_EVENT;
    }

    @Override public String toString() {
        return "UpdateBrokerInfoEvent{" +
            "brokerName='" + brokerName + '\'' +
            ", brokerIdentity='" + brokerIdentity + '\'' +
            ", brokerAddress='" + brokerAddress + '\'' +
            '}';
    }
}
