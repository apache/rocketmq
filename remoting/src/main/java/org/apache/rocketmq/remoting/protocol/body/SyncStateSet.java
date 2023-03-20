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

import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class SyncStateSet extends RemotingSerializable {
    private Set<Long> syncStateSet;
    private int syncStateSetEpoch;

    public SyncStateSet(Set<Long> syncStateSet, int syncStateSetEpoch) {
        this.syncStateSet = new HashSet<>(syncStateSet);
        this.syncStateSetEpoch = syncStateSetEpoch;
    }

    public Set<Long> getSyncStateSet() {
        return new HashSet<>(syncStateSet);
    }

    public void setSyncStateSet(Set<Long> syncStateSet) {
        this.syncStateSet = new HashSet<>(syncStateSet);
    }

    public int getSyncStateSetEpoch() {
        return syncStateSetEpoch;
    }

    public void setSyncStateSetEpoch(int syncStateSetEpoch) {
        this.syncStateSetEpoch = syncStateSetEpoch;
    }

    @Override
    public String toString() {
        return "SyncStateSet{" +
            "syncStateSet=" + syncStateSet +
            ", syncStateSetEpoch=" + syncStateSetEpoch +
            '}';
    }
}
