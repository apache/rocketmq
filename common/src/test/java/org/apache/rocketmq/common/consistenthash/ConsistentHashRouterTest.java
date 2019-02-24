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
package org.apache.rocketmq.common.consistenthash;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *  Created by guoyao on 2019/2/22.
 */
public class ConsistentHashRouterTest {

    private String KEY_A="a";
    private String KEY_B="b";
    private String KEY_C="c";
    private String KEY_D="d";
    private String NOT_EXIST_KEY="not_exist_key";
    private int defaultReplicas = 10;

    @Test(expected = NullPointerException.class)
    public void testConstrctorNullHashFunctionWithException() {
        new ConsistentHashRouter<Node>(null, 0, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstrctorNegativeVNodeCountWithException() {
        new ConsistentHashRouter<Node>(getNodes(), -1);
    }

    @Test(expected = NullPointerException.class)
    public void testAddNodeNullPNodeWithException() {
        ConsistentHashRouter hashRouter=buildDefaultConsistentHashRouter(defaultReplicas);
        hashRouter.addNode(null, defaultReplicas);
    }

    @Test
    public void testAddNode() {
        ConsistentHashRouter hashRouter=buildDefaultConsistentHashRouter(defaultReplicas);
        hashRouter.addNode(createNodeByKey(KEY_D), defaultReplicas);
    }

    @Test
    public void testGetExistingReplicas() {
        ConsistentHashRouter defaultRouter=buildDefaultConsistentHashRouter(defaultReplicas);
        assertThat(defaultRouter.getExistingReplicas(createNodeByKey(KEY_A))).isEqualTo(defaultReplicas);
        assertThat(defaultRouter.getExistingReplicas(createNodeByKey(KEY_B))).isEqualTo(defaultReplicas);
        assertThat(defaultRouter.getExistingReplicas(createNodeByKey(KEY_C))).isEqualTo(defaultReplicas);
        int replicas = 15;
        defaultRouter.addNode(createNodeByKey(KEY_D), replicas);
        assertThat(defaultRouter.getExistingReplicas(createNodeByKey(KEY_D))).isEqualTo(replicas);
    }

    @Test
    public void testRouteNode() {
        ConsistentHashRouter defaultRouter=buildDefaultConsistentHashRouter(defaultReplicas);
        assertThat(defaultRouter.routeNode(NOT_EXIST_KEY)).isNotNull();
        assertThat(defaultRouter.routeNode(KEY_A)).isEqualTo(defaultRouter.routeNode(KEY_A));
        assertThat(defaultRouter.routeNode(KEY_B)).isEqualTo(defaultRouter.routeNode(KEY_B));
        assertThat(defaultRouter.routeNode(KEY_C)).isEqualTo(defaultRouter.routeNode(KEY_C));
    }

    @Test
    public void testRemoveNode() {
        ConsistentHashRouter defaultRouter=buildDefaultConsistentHashRouter(defaultReplicas);
        defaultRouter.removeNode(createNodeByKey(KEY_A));
        assertThat(defaultRouter.getExistingReplicas(createNodeByKey(KEY_A))).isEqualTo(0);
    }

    private ConsistentHashRouter buildDefaultConsistentHashRouter(int replicas) {
        List<Node> nodes=getNodes();
        return new ConsistentHashRouter<Node>(nodes, replicas);
    }

    private List<Node> getNodes() {
        List<Node> nodes=new ArrayList<Node>();
        nodes.add(createNodeByKey(KEY_A));
        nodes.add(createNodeByKey(KEY_B));
        nodes.add(createNodeByKey(KEY_C));
        return nodes;
    }

    private Node createNodeByKey(final String key) {
        assertThat(key).isNotNull();
        return new Node() {
            @Override
            public String getKey() {
                return key;
            }
        };
    }
}
