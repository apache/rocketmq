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

/**
 * $Id: SubscriptionData.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.heartbeat;

public class MqttSubscriptionData extends SubscriptionData {
    private int qos;
    private String clientId;

    public MqttSubscriptionData() {

    }

    public MqttSubscriptionData(int qos, String clientId, String topicFilter) {
        super(topicFilter, null);
        this.qos = qos;
        this.clientId = clientId;
    }

    public int getQos() {
        return qos;
    }

    public void setQos(int qos) {
        this.qos = qos;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.getTopic() == null) ? 0 : this.getTopic().hashCode());
        result = prime * result + ((clientId == null) ? 0 : clientId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MqttSubscriptionData other = (MqttSubscriptionData) obj;
        if (clientId != other.clientId) {
            return false;
        }
        if (this.getTopic() != other.getTopic()) {
            return false;
        }
        return true;
    }

    @Override public String toString() {
        return "MqttSubscriptionData{" +
            "qos=" + qos +
            ", topic='" + this.getTopic() + '\'' +
            ", clientId='" + clientId + '\'' +
            '}';
    }
}
