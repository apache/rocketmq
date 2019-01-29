package org.apache.rocketmq.common.protocol.body;/*
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

import java.util.HashMap;
import java.util.Set;
import org.apache.rocketmq.common.protocol.route.SnodeData;
import org.apache.rocketmq.remoting.serialize.RemotingSerializable;

public class SnodeClusterInfo extends RemotingSerializable {
    private HashMap<String/* snodeName*/, SnodeData> snodeTable;
    private HashMap<String/* clusterName*/, Set<String/*snodeName*/>> snodeCluster;

    public HashMap<String, SnodeData> getSnodeTable() {
        return snodeTable;
    }

    public void setSnodeTable(
        HashMap<String, SnodeData> snodeTable) {
        this.snodeTable = snodeTable;
    }

    public HashMap<String, Set<String>> getSnodeCluster() {
        return snodeCluster;
    }

    public void setSnodeCluster(HashMap<String, Set<String>> snodeCluster) {
        this.snodeCluster = snodeCluster;
    }
}
