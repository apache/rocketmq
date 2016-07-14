/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/**
 * $Id: StoreCheckpointTest.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.store;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertTrue;


public class StoreCheckpointTest {
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {

    }


    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }


    @Test
    public void test_write_read() {
        try {
            StoreCheckpoint storeCheckpoint = new StoreCheckpoint("./a/b/0000");
            long physicMsgTimestamp = 0xAABB;
            long logicsMsgTimestamp = 0xCCDD;
            storeCheckpoint.setPhysicMsgTimestamp(physicMsgTimestamp);
            storeCheckpoint.setLogicsMsgTimestamp(logicsMsgTimestamp);
            storeCheckpoint.flush();

            long diff = physicMsgTimestamp - storeCheckpoint.getMinTimestamp();
            assertTrue(diff == 3000);

            storeCheckpoint.shutdown();

            storeCheckpoint = new StoreCheckpoint("a/b/0000");
            assertTrue(physicMsgTimestamp == storeCheckpoint.getPhysicMsgTimestamp());
            assertTrue(logicsMsgTimestamp == storeCheckpoint.getLogicsMsgTimestamp());
        } catch (Throwable e) {
            e.printStackTrace();
            assertTrue(false);
        }

    }
}
