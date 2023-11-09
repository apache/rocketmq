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
package org.apache.rocketmq.remoting.protocol.header;

import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RequestCode;

@RocketMQAction(value = RequestCode.NOTIFY_BROKER_ROLE_CHANGED, resource = ResourceType.CLUSTER, action = Action.UPDATE)
public class NotifyBrokerRoleChangedRequestHeader implements CommandCustomHeader {
    private String masterAddress;
    private Integer masterEpoch;
    private Integer syncStateSetEpoch;
    private Long masterBrokerId;

    public NotifyBrokerRoleChangedRequestHeader() {
    }

    public NotifyBrokerRoleChangedRequestHeader(String masterAddress, Long masterBrokerId, Integer masterEpoch, Integer syncStateSetEpoch) {
        this.masterAddress = masterAddress;
        this.masterEpoch = masterEpoch;
        this.syncStateSetEpoch = syncStateSetEpoch;
        this.masterBrokerId = masterBrokerId;
    }

    public String getMasterAddress() {
        return masterAddress;
    }

    public void setMasterAddress(String masterAddress) {
        this.masterAddress = masterAddress;
    }

    public Integer getMasterEpoch() {
        return masterEpoch;
    }

    public void setMasterEpoch(Integer masterEpoch) {
        this.masterEpoch = masterEpoch;
    }

    public Integer getSyncStateSetEpoch() {
        return syncStateSetEpoch;
    }

    public void setSyncStateSetEpoch(Integer syncStateSetEpoch) {
        this.syncStateSetEpoch = syncStateSetEpoch;
    }

    public Long getMasterBrokerId() {
        return masterBrokerId;
    }

    public void setMasterBrokerId(Long masterBrokerId) {
        this.masterBrokerId = masterBrokerId;
    }

    @Override
    public String toString() {
        return "NotifyBrokerRoleChangedRequestHeader{" +
                "masterAddress='" + masterAddress + '\'' +
                ", masterEpoch=" + masterEpoch +
                ", syncStateSetEpoch=" + syncStateSetEpoch +
                ", masterBrokerId=" + masterBrokerId +
                '}';
    }

    @Override
    public void checkFields() throws RemotingCommandException {

    }
}
