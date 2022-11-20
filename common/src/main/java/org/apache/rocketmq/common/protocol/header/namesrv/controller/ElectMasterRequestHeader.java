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
package org.apache.rocketmq.common.protocol.header.namesrv.controller;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class ElectMasterRequestHeader implements CommandCustomHeader {

    @CFNotNull
    private String clusterName;

    @CFNotNull
    private String brokerName;

    @CFNotNull
    private String brokerAddress;

    public ElectMasterRequestHeader() {
    }

    public ElectMasterRequestHeader(String brokerName) {
        this.brokerName = brokerName;
    }

    public ElectMasterRequestHeader(String clusterName, String brokerName, String brokerAddress) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.brokerAddress = brokerAddress;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public String getBrokerAddress() {
        return brokerAddress;
    }

    public void setBrokerAddress(String brokerAddress) {
        this.brokerAddress = brokerAddress;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    @Override
    public String toString() {
        return "ElectMasterRequestHeader{" +
            "clusterName='" + clusterName + '\'' +
            "brokerName='" + brokerName + '\'' +
            "brokerAddress='" + brokerAddress + '\'' +
            '}';
    }

    @Override
    public void checkFields() throws RemotingCommandException {
    }
}
