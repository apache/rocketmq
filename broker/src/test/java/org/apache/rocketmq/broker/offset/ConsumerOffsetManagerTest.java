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

/**
 * $Id: ConsumerOffsetManagerTest.java 1831 2013-05-16 01:39:51Z vintagewang@apache.org $
 */
package org.apache.rocketmq.broker.offset;

import org.apache.rocketmq.broker.BrokerTestHarness;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ConsumerOffsetManagerTest extends BrokerTestHarness {

    @Test
    public void testFlushConsumerOffset() throws Exception {
        ConsumerOffsetManager consumerOffsetManager = new ConsumerOffsetManager(brokerController);
        for (int i = 0; i < 10; i++) {
            String group = "UNIT_TEST_GROUP_" + i;
            for (int id = 0; id < 10; id++) {
                consumerOffsetManager.commitOffset(null, group, "TOPIC_A", id, id + 100);
                consumerOffsetManager.commitOffset(null, group, "TOPIC_B", id, id + 100);
                consumerOffsetManager.commitOffset(null, group, "TOPIC_C", id, id + 100);
            }
        }
        consumerOffsetManager.persist();
        consumerOffsetManager.getOffsetTable().clear();
        for (int i = 0; i < 10; i++) {
            String group = "UNIT_TEST_GROUP_" + i;
            for (int id = 0; id < 10; id++) {
                assertEquals(consumerOffsetManager.queryOffset(group, "TOPIC_A", id), -1);
                assertEquals(consumerOffsetManager.queryOffset(group, "TOPIC_B", id), -1);
                assertEquals(consumerOffsetManager.queryOffset(group, "TOPIC_B", id), -1);
            }
        }
        consumerOffsetManager.load();
        for (int i = 0; i < 10; i++) {
            String group = "UNIT_TEST_GROUP_" + i;
            for (int id = 0; id < 10; id++) {
                assertEquals(consumerOffsetManager.queryOffset(group, "TOPIC_A", id), id + 100);
                assertEquals(consumerOffsetManager.queryOffset(group, "TOPIC_B", id), id + 100);
                assertEquals(consumerOffsetManager.queryOffset(group, "TOPIC_B", id), id + 100);
            }
        }
    }
}
