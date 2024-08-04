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
package org.apache.rocketmq.controller.impl.manager;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.controller.impl.event.ControllerResult;
import org.apache.rocketmq.controller.impl.heartbeat.BrokerIdentityInfo;
import org.apache.rocketmq.controller.impl.heartbeat.BrokerLiveInfo;
import org.apache.rocketmq.controller.impl.task.CheckNotActiveBrokerRequest;
import org.apache.rocketmq.controller.impl.task.CheckNotActiveBrokerResponse;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


import static org.apache.rocketmq.controller.ControllerTestBase.DEFAULT_BROKER_NAME;
import static org.apache.rocketmq.controller.ControllerTestBase.DEFAULT_CLUSTER_NAME;
import static org.apache.rocketmq.controller.ControllerTestBase.DEFAULT_IP;
import static org.junit.Assert.assertEquals;

public class RaftReplicasInfoManagerTest extends ReplicasInfoManagerTest {
    private RaftReplicasInfoManager raftReplicasInfoManager;

    @Before
    public void init() {
        super.init();
        raftReplicasInfoManager = new RaftReplicasInfoManager(config);
        replicasInfoManager = raftReplicasInfoManager;
    }

    @Test
    public void testCheckNotActiveBroker() throws NoSuchFieldException, IllegalAccessException {
        CheckNotActiveBrokerRequest request = new CheckNotActiveBrokerRequest();
        long checkTimeMillis = request.getCheckTimeMillis() - 1000;
        mockMetaData();
        Field field = RaftReplicasInfoManager.class.getDeclaredField("brokerLiveTable");
        field.setAccessible(true);
        Map<BrokerIdentityInfo, BrokerLiveInfo> brokerLiveTable = (Map) field.get(raftReplicasInfoManager);
        BrokerIdentityInfo broker1 = new BrokerIdentityInfo(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, 1L);
        BrokerLiveInfo brokerLiveInfo1 = new BrokerLiveInfo(DEFAULT_BROKER_NAME, DEFAULT_IP[0], 1L, checkTimeMillis, 0L, null, 1, 0L, 1);
        BrokerIdentityInfo broker2 = new BrokerIdentityInfo(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, 2L);
        BrokerLiveInfo brokerLiveInfo2 = new BrokerLiveInfo(DEFAULT_BROKER_NAME, DEFAULT_IP[0], 1L, checkTimeMillis, 1000 * 60 * 20L, null, 1, 0L, 1);
        BrokerIdentityInfo broker3 = new BrokerIdentityInfo(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, 3L);
        BrokerLiveInfo brokerLiveInfo3 = new BrokerLiveInfo(DEFAULT_BROKER_NAME, DEFAULT_IP[0], 1L, checkTimeMillis, 1000 * 60 * 20L, null, 1, 0L, 1);
        brokerLiveTable.put(broker1, brokerLiveInfo1);
        brokerLiveTable.put(broker2, brokerLiveInfo2);
        brokerLiveTable.put(broker3, brokerLiveInfo3);
        ControllerResult<CheckNotActiveBrokerResponse> result = raftReplicasInfoManager.checkNotActiveBroker(request);
        List<BrokerIdentityInfo> list2 = new ArrayList<>();
        list2.add(broker1);
        assertEquals(new String(result.getBody()), new String(JSON.toJSONBytes(list2)));
    }
}
