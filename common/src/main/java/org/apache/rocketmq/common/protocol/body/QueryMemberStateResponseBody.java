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

package org.apache.rocketmq.common.protocol.body;

import java.util.Map;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class QueryMemberStateResponseBody extends RemotingSerializable {
    private  String group;
    private  String selfId;
    private  String leaderId;
    private  long currTerm;
    private Map<String, String> peerMap;
    private Map<String, Boolean> peersLiveTable;

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getSelfId() {
        return selfId;
    }

    public void setSelfId(String selfId) {
        this.selfId = selfId;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public long getCurrTerm() {
        return currTerm;
    }

    public void setCurrTerm(long currTerm) {
        this.currTerm = currTerm;
    }

    public Map<String, String> getPeerMap() {
        return peerMap;
    }

    public void setPeerMap(Map<String, String> peerMap) {
        this.peerMap = peerMap;
    }

    public Map<String, Boolean> getPeersLiveTable() {
        return peersLiveTable;
    }

    public void setPeersLiveTable(Map<String, Boolean> peersLiveTable) {
        this.peersLiveTable = peersLiveTable;
    }
}

