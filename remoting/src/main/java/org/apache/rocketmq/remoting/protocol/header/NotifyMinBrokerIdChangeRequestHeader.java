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
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RequestCode;

@RocketMQAction(value = RequestCode.NOTIFY_MIN_BROKER_ID_CHANGE, action = Action.UPDATE)
public class NotifyMinBrokerIdChangeRequestHeader implements CommandCustomHeader {
    @CFNullable
    private Long minBrokerId;

    @CFNullable
    private String brokerName;

    @CFNullable
    private String minBrokerAddr;

    @CFNullable
    private String offlineBrokerAddr;

    @CFNullable
    private String haBrokerAddr;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public Long getMinBrokerId() {
        return minBrokerId;
    }

    public void setMinBrokerId(Long minBrokerId) {
        this.minBrokerId = minBrokerId;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public String getMinBrokerAddr() {
        return minBrokerAddr;
    }

    public void setMinBrokerAddr(String minBrokerAddr) {
        this.minBrokerAddr = minBrokerAddr;
    }

    public String getOfflineBrokerAddr() {
        return offlineBrokerAddr;
    }

    public void setOfflineBrokerAddr(String offlineBrokerAddr) {
        this.offlineBrokerAddr = offlineBrokerAddr;
    }

    public String getHaBrokerAddr() {
        return haBrokerAddr;
    }

    public void setHaBrokerAddr(String haBrokerAddr) {
        this.haBrokerAddr = haBrokerAddr;
    }
}
