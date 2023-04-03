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
package org.apache.rocketmq.remoting.protocol.header.controller;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class ElectMasterRequestHeader implements CommandCustomHeader {

    @CFNotNull
    private String clusterName;

    @CFNotNull
    private String brokerName;

    /**
     * brokerId
     * for brokerTrigger electMaster: this brokerId will be elected as a master when it is the first time to elect
     * in this broker-set
     * for adminTrigger electMaster: this brokerId is also named assignedBrokerId, which means we must prefer to elect
     * it as a new master when this broker is valid.
     */
    @CFNotNull
    private Long brokerId;

    @CFNotNull
    private Boolean designateElect = false;

    public ElectMasterRequestHeader() {
    }

    public ElectMasterRequestHeader(String brokerName) {
        this.brokerName = brokerName;
    }

    public ElectMasterRequestHeader(String clusterName, String brokerName, Long brokerId) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.brokerId = brokerId;
    }

    public ElectMasterRequestHeader(String clusterName, String brokerName, Long brokerId, boolean designateElect) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.brokerId = brokerId;
        this.designateElect = designateElect;
    }

    public static ElectMasterRequestHeader ofBrokerTrigger(String clusterName, String brokerName,
        Long brokerId) {
        return new ElectMasterRequestHeader(clusterName, brokerName, brokerId);
    }

    public static ElectMasterRequestHeader ofControllerTrigger(String brokerName) {
        return new ElectMasterRequestHeader(brokerName);
    }

    public static ElectMasterRequestHeader ofAdminTrigger(String clusterName, String brokerName, Long brokerId) {
        return new ElectMasterRequestHeader(clusterName, brokerName, brokerId, true);
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public Long getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(Long brokerId) {
        this.brokerId = brokerId;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public boolean getDesignateElect() {
        return this.designateElect;
    }

    @Override
    public String toString() {
        return "ElectMasterRequestHeader{" +
                "clusterName='" + clusterName + '\'' +
                ", brokerName='" + brokerName + '\'' +
                ", brokerId=" + brokerId +
                ", designateElect=" + designateElect +
                '}';
    }

    @Override
    public void checkFields() throws RemotingCommandException {
    }
}
