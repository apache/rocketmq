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

package org.apache.rocketmq.controller.impl.event;

import java.util.Set;

public class CleanBrokerDataEvent implements EventMessage {

    private String brokerName;

    private Set<String> brokerAddressSet;

    public CleanBrokerDataEvent(String brokerName, Set<String> brokerAddressSet) {
        this.brokerName = brokerName;
        this.brokerAddressSet = brokerAddressSet;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public Set<String> getBrokerAddressSet() {
        return brokerAddressSet;
    }

    public void setBrokerAddressSet(Set<String> brokerAddressSet) {
        this.brokerAddressSet = brokerAddressSet;
    }

    /**
     * Returns the event type of this message
     */
    @Override
    public EventType getEventType() {
        return EventType.CLEAN_BROKER_DATA_EVENT;
    }

    @Override
    public String toString() {
        return "CleanBrokerDataEvent{" +
            "brokerName='" + brokerName + '\'' +
            ", brokerAddressSet=" + brokerAddressSet +
            '}';
    }
}
