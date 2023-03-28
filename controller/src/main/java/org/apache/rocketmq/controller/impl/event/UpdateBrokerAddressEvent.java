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

public class UpdateBrokerAddressEvent implements EventMessage {

    private String clusterName;

    private String brokerName;

    private String brokerAddress;

    private Long brokerId;

    public UpdateBrokerAddressEvent(String clusterName, String brokerName, String brokerAddress, Long brokerId) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.brokerAddress = brokerAddress;
        this.brokerId = brokerId;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public String getBrokerAddress() {
        return brokerAddress;
    }

    public Long getBrokerId() {
        return brokerId;
    }

    @Override
    public String toString() {
        return "UpdateBrokerAddressEvent{" +
                "clusterName='" + clusterName + '\'' +
                ", brokerName='" + brokerName + '\'' +
                ", brokerAddress='" + brokerAddress + '\'' +
                ", brokerId=" + brokerId +
                '}';
    }

    @Override
    public EventType getEventType() {
        return EventType.UPDATE_BROKER_ADDRESS;
    }
}
