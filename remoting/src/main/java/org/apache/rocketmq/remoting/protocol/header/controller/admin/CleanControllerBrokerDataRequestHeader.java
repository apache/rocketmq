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

package org.apache.rocketmq.remoting.protocol.header.controller.admin;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class CleanControllerBrokerDataRequestHeader implements CommandCustomHeader {

    @CFNullable
    private String clusterName;

    @CFNotNull
    private String brokerName;

    @CFNullable
    private String brokerControllerIdsToClean;

    private boolean isCleanLivingBroker = false;

    public CleanControllerBrokerDataRequestHeader() {
    }

    public CleanControllerBrokerDataRequestHeader(String clusterName, String brokerName, String brokerIdSetToClean,
        boolean isCleanLivingBroker) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.brokerControllerIdsToClean = brokerIdSetToClean;
        this.isCleanLivingBroker = isCleanLivingBroker;
    }

    public CleanControllerBrokerDataRequestHeader(String clusterName, String brokerName, String brokerIdSetToClean) {
        this(clusterName, brokerName, brokerIdSetToClean, false);
    }

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public String getBrokerControllerIdsToClean() {
        return brokerControllerIdsToClean;
    }

    public void setBrokerControllerIdsToClean(String brokerIdSetToClean) {
        this.brokerControllerIdsToClean = brokerIdSetToClean;
    }

    public boolean isCleanLivingBroker() {
        return isCleanLivingBroker;
    }

    public void setCleanLivingBroker(boolean cleanLivingBroker) {
        isCleanLivingBroker = cleanLivingBroker;
    }

    @Override
    public String toString() {
        return "CleanControllerBrokerDataRequestHeader{" +
                "clusterName='" + clusterName + '\'' +
                ", brokerName='" + brokerName + '\'' +
                ", brokerIdSetToClean='" + brokerControllerIdsToClean + '\'' +
                ", isCleanLivingBroker=" + isCleanLivingBroker +
                '}';
    }
}
