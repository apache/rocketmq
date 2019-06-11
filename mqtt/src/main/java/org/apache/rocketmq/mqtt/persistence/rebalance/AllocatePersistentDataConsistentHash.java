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

package org.apache.rocketmq.mqtt.persistence.rebalance;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import org.apache.rocketmq.common.consistenthash.ConsistentHashRouter;
import org.apache.rocketmq.common.consistenthash.HashFunction;
import org.apache.rocketmq.common.consistenthash.Node;

public class AllocatePersistentDataConsistentHash implements AllocatePersistentDataStrategy {
    private final int virtualNodeCnt;
    private final HashFunction customHashFunction;

    public AllocatePersistentDataConsistentHash() {
        this(10);
    }
    public AllocatePersistentDataConsistentHash(int virtualNodeCnt) {
        this(virtualNodeCnt,null);
    }
    public AllocatePersistentDataConsistentHash(int virtualNodeCnt,HashFunction customHashFunction) {
        if (virtualNodeCnt < 0) {
            throw new IllegalArgumentException("illegal virtualNodeCnt :" + virtualNodeCnt);
        }
        this.virtualNodeCnt = virtualNodeCnt;
        this.customHashFunction = customHashFunction;
    }
    @Override
    public String allocate(String dataKey, Set<String> enodeNames) {
        Collection<ClientNode> cidNodes = new ArrayList<ClientNode>();
        for (String enodeName:enodeNames) {
            cidNodes.add(new ClientNode(enodeName));
        }
        final ConsistentHashRouter<ClientNode> router; //for building hash ring
        if (customHashFunction != null) {
            router = new ConsistentHashRouter<ClientNode>(cidNodes, virtualNodeCnt, customHashFunction);
        } else {
            router = new ConsistentHashRouter<ClientNode>(cidNodes, virtualNodeCnt);
        }

        ClientNode clientNode = router.routeNode(dataKey);
        if (clientNode != null) {
            return clientNode.getKey();
        }
        return null;
    }
    @Override
    public String getName() {
        return "CONSISTENT_HASH";
    }
    private static class ClientNode implements Node {
        private final String clientID;

        public ClientNode(String clientID) {
            this.clientID = clientID;
        }

        @Override
        public String getKey() {
            return clientID;
        }
    }
}
