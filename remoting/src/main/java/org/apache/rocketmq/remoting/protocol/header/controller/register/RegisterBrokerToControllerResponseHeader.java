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

package org.apache.rocketmq.remoting.protocol.header.controller.register;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class RegisterBrokerToControllerResponseHeader implements CommandCustomHeader {

    private String clusterName;

    private String brokerName;

    private Long masterBrokerId;

    private String masterAddress;

    private Integer masterEpoch;

    private Integer syncStateSetEpoch;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public RegisterBrokerToControllerResponseHeader() {
    }

    public RegisterBrokerToControllerResponseHeader(String clusterName, String brokerName) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
    }

    public void setMasterBrokerId(Long masterBrokerId) {
        this.masterBrokerId = masterBrokerId;
    }

    public void setMasterAddress(String masterAddress) {
        this.masterAddress = masterAddress;
    }

    public void setMasterEpoch(Integer masterEpoch) {
        this.masterEpoch = masterEpoch;
    }

    public void setSyncStateSetEpoch(Integer syncStateSetEpoch) {
        this.syncStateSetEpoch = syncStateSetEpoch;
    }

    public Integer getMasterEpoch() {
        return masterEpoch;
    }

    public Integer getSyncStateSetEpoch() {
        return syncStateSetEpoch;
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

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }
}
