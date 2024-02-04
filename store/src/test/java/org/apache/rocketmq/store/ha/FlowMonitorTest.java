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

package org.apache.rocketmq.store.ha;

import java.time.Duration;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Assert;
import org.junit.Test;

import static org.awaitility.Awaitility.await;

public class FlowMonitorTest {

    @Test
    public void testLimit() throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setHaFlowControlEnable(true);
        messageStoreConfig.setMaxHaTransferByteInSecond(10);

        FlowMonitor flowMonitor = new FlowMonitor(messageStoreConfig);
        flowMonitor.start();

        flowMonitor.addByteCountTransferred(3);
        Boolean flag = await().atMost(Duration.ofSeconds(2)).until(() -> 7 == flowMonitor.canTransferMaxByteNum(), item -> item);
        flag &= await().atMost(Duration.ofSeconds(2)).until(() -> 10 == flowMonitor.canTransferMaxByteNum(), item -> item);
        Assert.assertTrue(flag);

        flowMonitor.shutdown();
    }

    @Test
    public void testSpeed() throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setHaFlowControlEnable(true);
        messageStoreConfig.setMaxHaTransferByteInSecond(10);

        FlowMonitor flowMonitor = new FlowMonitor(messageStoreConfig);

        flowMonitor.addByteCountTransferred(3);
        flowMonitor.calculateSpeed();
        Assert.assertEquals(3, flowMonitor.getTransferredByteInSecond());

        flowMonitor.addByteCountTransferred(5);
        flowMonitor.calculateSpeed();
        Assert.assertEquals(5, flowMonitor.getTransferredByteInSecond());
    }
}
