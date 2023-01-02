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
package org.apache.rocketmq.controller.impl.controller.impl.statemachine;

import org.apache.rocketmq.controller.impl.manager.BrokerInfo;
import org.apache.rocketmq.controller.impl.manager.ReplicasInfoManager;
import org.apache.rocketmq.controller.impl.manager.SyncStateInfo;
import org.apache.rocketmq.controller.impl.statemachine.StatemachineSnapshotFileGenerator;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class StatemachineSnapshotFileGeneratorTest {

    public String snapshotPath;
    StatemachineSnapshotFileGenerator snapshotGenerator;
    ReplicasInfoManager replicasInfoManager;


    public void mockMetadata() {
        BrokerInfo broker1 = new BrokerInfo("broker1", "cluster1");
        broker1.setBrokerIdTable(new HashMap<String, Long>() {{
                put("127.0.0.1:10000", 1L);
                put("127.0.0.1:10001", 2L);
            }});
        broker1.setBrokerIdCount(2L);

        SyncStateInfo syncStateInfo1 = new SyncStateInfo("cluster1", "broker1", "127.0.0.1:10000");
        syncStateInfo1.setSyncStateSet(new HashSet<String>() {{
                add("127.0.0.1:10000");
                add("127.0.0.1:10001");
            }});

        this.replicasInfoManager.setReplicaInfoTable(new HashMap<String, BrokerInfo>() {{
                put("broker1", broker1);
            }});
        this.replicasInfoManager.setSyncStateSetInfoTable(new HashMap<String, SyncStateInfo>() {{
                put("broker1", syncStateInfo1);
            }});
    }

    @Before
    public void init() {
        this.snapshotPath = Paths.get(File.separator + "tmp", "ControllerSnapshot").toString();
        File file = new File(snapshotPath);
        File parentFile = file.getParentFile();
        if (parentFile != null) {
            parentFile.mkdirs();
        }
        this.replicasInfoManager = new ReplicasInfoManager(null);
        this.snapshotGenerator = new StatemachineSnapshotFileGenerator(Collections.singletonList(this.replicasInfoManager));
    }

    @Test
    public void testGenerateAndLoadEmptySnapshot() throws IOException {
        ReplicasInfoManager emptyManager = new ReplicasInfoManager(null);
        StatemachineSnapshotFileGenerator generator1 = new StatemachineSnapshotFileGenerator(Collections.singletonList(emptyManager));
        generator1.generateSnapshot(this.snapshotPath);

        assertTrue(generator1.loadSnapshot(this.snapshotPath));
    }

    @Test
    public void testGenerateAndLoadSnapshot() throws IOException {
        mockMetadata();
        this.snapshotGenerator.generateSnapshot(this.snapshotPath);

        ReplicasInfoManager emptyManager = new ReplicasInfoManager(null);
        StatemachineSnapshotFileGenerator generator1 = new StatemachineSnapshotFileGenerator(Collections.singletonList(emptyManager));
        assertTrue(generator1.loadSnapshot(this.snapshotPath));

        assertArrayEquals(emptyManager.encodeMetadata(), this.replicasInfoManager.encodeMetadata());
    }
}