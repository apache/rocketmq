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

package org.apache.rocketmq.remoting.protocol.body;

import com.google.common.base.Objects;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import java.util.HashSet;
import java.util.Set;

public class ElectMasterResponseBody extends RemotingSerializable {
    private BrokerMemberGroup brokerMemberGroup;
    private Set<Long> syncStateSet;

    // Provide default constructor for serializer
    public ElectMasterResponseBody() {
        this.syncStateSet = new HashSet<Long>();
        this.brokerMemberGroup = null;
    }

    public ElectMasterResponseBody(final Set<Long> syncStateSet) {
        this.syncStateSet = syncStateSet;
        this.brokerMemberGroup = null;
    }

    public ElectMasterResponseBody(final BrokerMemberGroup brokerMemberGroup, final Set<Long> syncStateSet) {
        this.brokerMemberGroup = brokerMemberGroup;
        this.syncStateSet = syncStateSet;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ElectMasterResponseBody that = (ElectMasterResponseBody) o;
        return Objects.equal(brokerMemberGroup, that.brokerMemberGroup) &&
            Objects.equal(syncStateSet, that.syncStateSet);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(brokerMemberGroup, syncStateSet);
    }

    @Override
    public String toString() {
        return "BrokerMemberGroup{" +
            "brokerMemberGroup='" + brokerMemberGroup.toString() + '\'' +
            ", syncStateSet='" + syncStateSet.toString() +
            '}';
    }

    public void setBrokerMemberGroup(BrokerMemberGroup brokerMemberGroup) {
        this.brokerMemberGroup = brokerMemberGroup;
    }

    public BrokerMemberGroup getBrokerMemberGroup() {
        return brokerMemberGroup;
    }

    public void setSyncStateSet(Set<Long> syncStateSet) {
        this.syncStateSet = syncStateSet;
    }

    public Set<Long> getSyncStateSet() {
        return syncStateSet;
    }
}
