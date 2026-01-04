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
package org.apache.rocketmq.controller.impl.task;

import org.apache.rocketmq.controller.impl.heartbeat.BrokerIdentityInfo;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class BrokerCloseChannelRequest implements CommandCustomHeader {
    @CFNullable
    private String clusterName;

    @CFNullable
    private String brokerName;

    @CFNullable
    private Long brokerId;

    public BrokerCloseChannelRequest() {
        this.clusterName = null;
        this.brokerName = null;
        this.brokerId = null;
    }

    public BrokerCloseChannelRequest(BrokerIdentityInfo brokerIdentityInfo) {
        this.clusterName = brokerIdentityInfo.getClusterName();
        this.brokerName = brokerIdentityInfo.getBrokerName();
        this.brokerId = brokerIdentityInfo.getBrokerId();
    }

    public BrokerIdentityInfo getBrokerIdentityInfo() {
        return new BrokerIdentityInfo(this.clusterName, this.brokerName, this.brokerId);
    }

    public void setBrokerIdentityInfo(BrokerIdentityInfo brokerIdentityInfo) {
        this.clusterName = brokerIdentityInfo.getClusterName();
        this.brokerName = brokerIdentityInfo.getBrokerName();
        this.brokerId = brokerIdentityInfo.getBrokerId();
    }

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    @Override
    public String toString() {
        return "BrokerCloseChannelRequest{" +
            "clusterName='" + clusterName + '\'' +
            ", brokerName='" + brokerName + '\'' +
            ", brokerId=" + brokerId +
            '}';
    }
}
