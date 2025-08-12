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
 * $Id: StoreCheckpointTest.java 1831 2013-05-16 01:39:51Z vintagewang@apache.org $
 */
package org.apache.rocketmq.store;

import java.io.File;
import java.io.IOException;

import org.apache.rocketmq.common.UtilAll;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class StoreCheckpointTest {
    
    private static final String CHECKPOINT_DIR = "target/checkpoint_test";
    private static final String CHECKPOINT_FILE = CHECKPOINT_DIR + "/0000";
    
    @Before
    public void setUp() {
        File dir = new File(CHECKPOINT_DIR);
        if (dir.exists()) {
            UtilAll.deleteFile(dir);
        }
    }
    
    @Test
    public void testWriteAndRead() throws IOException {
        StoreCheckpoint storeCheckpoint = new StoreCheckpoint(CHECKPOINT_FILE);
        long physicMsgTimestamp = 0xAABB;
        long logicsMsgTimestamp = 0xCCDD;
        storeCheckpoint.setPhysicMsgTimestamp(physicMsgTimestamp);
        storeCheckpoint.setLogicsMsgTimestamp(logicsMsgTimestamp);
        storeCheckpoint.flush();

        long diff = physicMsgTimestamp - storeCheckpoint.getMinTimestamp();
        assertThat(diff).isEqualTo(3000);
        storeCheckpoint.shutdown();
        storeCheckpoint = new StoreCheckpoint(CHECKPOINT_FILE);
        assertThat(storeCheckpoint.getPhysicMsgTimestamp()).isEqualTo(physicMsgTimestamp);
        assertThat(storeCheckpoint.getLogicsMsgTimestamp()).isEqualTo(logicsMsgTimestamp);
    }
    
    @Test
    public void testCreateNewCheckpointFile() throws IOException {
        // Test creating a new checkpoint file when it doesn't exist
        StoreCheckpoint storeCheckpoint = new StoreCheckpoint(CHECKPOINT_FILE);
        
        // Default values should be 0
        assertThat(storeCheckpoint.getPhysicMsgTimestamp()).isEqualTo(0);
        assertThat(storeCheckpoint.getLogicsMsgTimestamp()).isEqualTo(0);
        assertThat(storeCheckpoint.getIndexMsgTimestamp()).isEqualTo(0);
        assertThat(storeCheckpoint.getMasterFlushedOffset()).isEqualTo(0);
        assertThat(storeCheckpoint.getConfirmPhyOffset()).isEqualTo(0);
        
        storeCheckpoint.shutdown();
    }
    
    @Test
    public void testAllFieldsPersistence() throws IOException {
        StoreCheckpoint storeCheckpoint = new StoreCheckpoint(CHECKPOINT_FILE);
        
        // Set all fields
        long physicMsgTimestamp = 1000L;
        long logicsMsgTimestamp = 2000L;
        long indexMsgTimestamp = 3000L;
        long masterFlushedOffset = 4000L;
        long confirmPhyOffset = 5000L;
        
        storeCheckpoint.setPhysicMsgTimestamp(physicMsgTimestamp);
        storeCheckpoint.setLogicsMsgTimestamp(logicsMsgTimestamp);
        storeCheckpoint.setIndexMsgTimestamp(indexMsgTimestamp);
        storeCheckpoint.setMasterFlushedOffset(masterFlushedOffset);
        storeCheckpoint.setConfirmPhyOffset(confirmPhyOffset);
        
        // Flush to disk
        storeCheckpoint.flush();
        storeCheckpoint.shutdown();
        
        // Read from disk
        StoreCheckpoint newCheckpoint = new StoreCheckpoint(CHECKPOINT_FILE);
        assertThat(newCheckpoint.getPhysicMsgTimestamp()).isEqualTo(physicMsgTimestamp);
        assertThat(newCheckpoint.getLogicsMsgTimestamp()).isEqualTo(logicsMsgTimestamp);
        assertThat(newCheckpoint.getIndexMsgTimestamp()).isEqualTo(indexMsgTimestamp);
        assertThat(newCheckpoint.getMasterFlushedOffset()).isEqualTo(masterFlushedOffset);
        assertThat(newCheckpoint.getConfirmPhyOffset()).isEqualTo(confirmPhyOffset);
        
        newCheckpoint.shutdown();
    }
    
    @Test
    public void testGetMinTimestamp() throws IOException {
        StoreCheckpoint storeCheckpoint = new StoreCheckpoint(CHECKPOINT_FILE);
        
        // Test case 1: physicMsgTimestamp < logicsMsgTimestamp
        storeCheckpoint.setPhysicMsgTimestamp(5000L);
        storeCheckpoint.setLogicsMsgTimestamp(8000L);
        assertThat(storeCheckpoint.getMinTimestamp()).isEqualTo(2000L); // 5000 - 3000 = 2000
        
        // Test case 2: logicsMsgTimestamp < physicMsgTimestamp
        storeCheckpoint.setPhysicMsgTimestamp(10000L);
        storeCheckpoint.setLogicsMsgTimestamp(7000L);
        assertThat(storeCheckpoint.getMinTimestamp()).isEqualTo(4000L); // 7000 - 3000 = 4000
        
        // Test case 3: result would be negative, should return 0
        storeCheckpoint.setPhysicMsgTimestamp(1000L);
        storeCheckpoint.setLogicsMsgTimestamp(2000L);
        assertThat(storeCheckpoint.getMinTimestamp()).isEqualTo(0); // 1000 - 3000 = -2000, but returns 0
        
        storeCheckpoint.shutdown();
    }
    
    @Test
    public void testGetMinTimestampIndex() throws IOException {
        StoreCheckpoint storeCheckpoint = new StoreCheckpoint(CHECKPOINT_FILE);
        
        // Test case 1: indexMsgTimestamp is the minimum
        storeCheckpoint.setPhysicMsgTimestamp(10000L);
        storeCheckpoint.setLogicsMsgTimestamp(8000L);
        storeCheckpoint.setIndexMsgTimestamp(3000L);
        assertThat(storeCheckpoint.getMinTimestampIndex()).isEqualTo(3000L);
        
        // Test case 2: getMinTimestamp() is the minimum
        storeCheckpoint.setPhysicMsgTimestamp(5000L);
        storeCheckpoint.setLogicsMsgTimestamp(6000L);
        storeCheckpoint.setIndexMsgTimestamp(10000L);
        assertThat(storeCheckpoint.getMinTimestampIndex()).isEqualTo(2000L); // min(5000, 6000) - 3000 = 2000
        
        storeCheckpoint.shutdown();
    }
    
    @Test
    public void testSettersAndGetters() throws IOException {
        StoreCheckpoint storeCheckpoint = new StoreCheckpoint(CHECKPOINT_FILE);
        
        // Test setters and getters without persistence
        long testValue = 12345L;
        
        storeCheckpoint.setPhysicMsgTimestamp(testValue);
        assertThat(storeCheckpoint.getPhysicMsgTimestamp()).isEqualTo(testValue);
        
        storeCheckpoint.setLogicsMsgTimestamp(testValue);
        assertThat(storeCheckpoint.getLogicsMsgTimestamp()).isEqualTo(testValue);
        
        storeCheckpoint.setIndexMsgTimestamp(testValue);
        assertThat(storeCheckpoint.getIndexMsgTimestamp()).isEqualTo(testValue);
        
        storeCheckpoint.setMasterFlushedOffset(testValue);
        assertThat(storeCheckpoint.getMasterFlushedOffset()).isEqualTo(testValue);
        
        storeCheckpoint.setConfirmPhyOffset(testValue);
        assertThat(storeCheckpoint.getConfirmPhyOffset()).isEqualTo(testValue);
        
        storeCheckpoint.shutdown();
    }
    
    @Test
    public void testMultipleFlushOperations() throws IOException {
        StoreCheckpoint storeCheckpoint = new StoreCheckpoint(CHECKPOINT_FILE);
        
        // First flush
        storeCheckpoint.setPhysicMsgTimestamp(1000L);
        storeCheckpoint.setLogicsMsgTimestamp(2000L);
        storeCheckpoint.flush();
        
        // Second flush with different values
        storeCheckpoint.setPhysicMsgTimestamp(3000L);
        storeCheckpoint.setLogicsMsgTimestamp(4000L);
        storeCheckpoint.flush();
        
        // Verify the latest values are persisted
        storeCheckpoint.shutdown();
        
        StoreCheckpoint newCheckpoint = new StoreCheckpoint(CHECKPOINT_FILE);
        assertThat(newCheckpoint.getPhysicMsgTimestamp()).isEqualTo(3000L);
        assertThat(newCheckpoint.getLogicsMsgTimestamp()).isEqualTo(4000L);
        newCheckpoint.shutdown();
    }
    
    @Test
    public void testShutdownIdempotency() throws IOException {
        StoreCheckpoint storeCheckpoint = new StoreCheckpoint(CHECKPOINT_FILE);
        
        // Set some values
        storeCheckpoint.setPhysicMsgTimestamp(1000L);
        storeCheckpoint.setLogicsMsgTimestamp(2000L);
        
        // Multiple shutdowns should not cause issues
        storeCheckpoint.shutdown();
        storeCheckpoint.shutdown(); // Second shutdown should be safe
        
        // Verify data is still persisted correctly
        StoreCheckpoint newCheckpoint = new StoreCheckpoint(CHECKPOINT_FILE);
        assertThat(newCheckpoint.getPhysicMsgTimestamp()).isEqualTo(1000L);
        assertThat(newCheckpoint.getLogicsMsgTimestamp()).isEqualTo(2000L);
        newCheckpoint.shutdown();
    }

    @After
    public void destroy() {
        File file = new File(CHECKPOINT_DIR);
        UtilAll.deleteFile(file);
    }
}
