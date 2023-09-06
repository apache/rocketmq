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
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class GetBrokerLiveInfoRequest implements CommandCustomHeader {
    private String clusterName;

    private String brokerName;

    private Long brokerId;

    public GetBrokerLiveInfoRequest() {
        this.clusterName = null;
        this.brokerName = null;
        this.brokerId = null;
    }

    /**
     * @param brokerIdentity The BrokerIdentityInfo that needs to be queried, if it is null, it means obtaining BrokerLiveInfo for all brokers
     */
    public GetBrokerLiveInfoRequest(BrokerIdentityInfo brokerIdentity) {
        this.clusterName = brokerIdentity.getClusterName();
        this.brokerName = brokerIdentity.getBrokerName();
        this.brokerId = brokerIdentity.getBrokerId();
    }

    public BrokerIdentityInfo getBrokerIdentity() {
        return new BrokerIdentityInfo(this.clusterName, this.brokerName, this.brokerId);
    }

    public void setBrokerIdentity(BrokerIdentityInfo brokerIdentity) {
        this.clusterName = brokerIdentity.getClusterName();
        this.brokerName = brokerIdentity.getBrokerName();
        this.brokerId = brokerIdentity.getBrokerId();
    }

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    @Override
    public String toString() {
        return "GetBrokerLiveInfoRequest{" +
            "brokerIdentity=" + getBrokerIdentity() +
            '}';
    }
}
