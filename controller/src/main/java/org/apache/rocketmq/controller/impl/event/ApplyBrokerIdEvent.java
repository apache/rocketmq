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

/**
 * The event trys to apply a new id for a new broker.
 * Triggered by the RegisterBrokerApi.
 */
public class ApplyBrokerIdEvent implements EventMessage {
    private final String clusterName;
    private final String brokerName;
    private final String brokerAddress;

    private final String registerCheckCode;

    private final long newBrokerId;

    public ApplyBrokerIdEvent(String clusterName, String brokerName, String brokerAddress, long newBrokerId, String registerCheckCode) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.brokerAddress = brokerAddress;
        this.newBrokerId = newBrokerId;
        this.registerCheckCode = registerCheckCode;
    }

    @Override
    public EventType getEventType() {
        return EventType.APPLY_BROKER_ID_EVENT;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public String getBrokerAddress() {
        return brokerAddress;
    }

    public long getNewBrokerId() {
        return newBrokerId;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getRegisterCheckCode() {
        return registerCheckCode;
    }

    @Override
    public String toString() {
        return "ApplyBrokerIdEvent{" +
                "clusterName='" + clusterName + '\'' +
                ", brokerName='" + brokerName + '\'' +
                ", brokerAddress='" + brokerAddress + '\'' +
                ", registerCheckCode='" + registerCheckCode + '\'' +
                ", newBrokerId=" + newBrokerId +
                '}';
    }
}
