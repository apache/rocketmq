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

package org.apache.rocketmq.controller.dledger.statemachine.event.write;

public class RegisterBrokerResult implements WriteEventResult {

    private String clusterName;

    private String brokerName;

    private Long masterBrokerId;

    private String masterAddress;

    private Integer masterEpoch;

    private Integer syncStateSetEpoch;

    public RegisterBrokerResult(String clusterName, String brokerName, Long masterBrokerId, String masterAddress,
        Integer masterEpoch, Integer syncStateSetEpoch) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.masterBrokerId = masterBrokerId;
        this.masterAddress = masterAddress;
        this.masterEpoch = masterEpoch;
        this.syncStateSetEpoch = syncStateSetEpoch;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public Long getMasterBrokerId() {
        return masterBrokerId;
    }

    public String getMasterAddress() {
        return masterAddress;
    }

    public Integer getMasterEpoch() {
        return masterEpoch;
    }

    public Integer getSyncStateSetEpoch() {
        return syncStateSetEpoch;
    }

    @Override
    public WriteEventType getEventType() {
        return WriteEventType.REGISTER_BROKER;
    }

}
