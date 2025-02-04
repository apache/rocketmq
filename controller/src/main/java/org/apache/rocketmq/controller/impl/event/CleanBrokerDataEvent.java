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

    private Set<Long> brokerIdSetToClean;

    public CleanBrokerDataEvent(String brokerName, Set<Long> brokerIdSetToClean) {
        this.brokerName = brokerName;
        this.brokerIdSetToClean = brokerIdSetToClean;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public void setBrokerIdSetToClean(Set<Long> brokerIdSetToClean) {
        this.brokerIdSetToClean = brokerIdSetToClean;
    }

    public Set<Long> getBrokerIdSetToClean() {
        return brokerIdSetToClean;
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
            ", brokerIdSetToClean=" + brokerIdSetToClean +
            '}';
    }
}
