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
 * The event trys to elect a new master for target broker.
 * Triggered by the ElectMasterApi.
 */
public class ElectMasterEvent implements EventMessage {
    // Mark whether a new master was elected.
    private final boolean newMasterElected;
    private final String brokerName;
    private final String newMasterAddress;
    private final String clusterName;

    public ElectMasterEvent(boolean newMasterElected, String brokerName) {
        this(newMasterElected, brokerName, "", "");
    }

    public ElectMasterEvent(String brokerName, String newMasterAddress) {
        this(true, brokerName, newMasterAddress, "");
    }

    public ElectMasterEvent(boolean newMasterElected, String brokerName, String newMasterAddress, String clusterName) {
        this.newMasterElected = newMasterElected;
        this.brokerName = brokerName;
        this.newMasterAddress = newMasterAddress;
        this.clusterName = clusterName;
    }

    @Override
    public EventType getEventType() {
        return EventType.ELECT_MASTER_EVENT;
    }

    public boolean getNewMasterElected() {
        return newMasterElected;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public String getNewMasterAddress() {
        return newMasterAddress;
    }

    public String getClusterName() {
        return clusterName;
    }

    @Override
    public String toString() {
        return "ElectMasterEvent{" +
            "isNewMasterElected=" + newMasterElected +
            ", brokerName='" + brokerName + '\'' +
            ", newMasterAddress='" + newMasterAddress + '\'' +
            ", clusterName='" + clusterName + '\'' +
            '}';
    }
}
