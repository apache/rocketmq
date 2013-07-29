/**
 * $Id: StoreCheckpointTest.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.store;

import static org.junit.Assert.assertTrue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


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

            assertTrue(physicMsgTimestamp == storeCheckpoint.getMinTimestamp());

            storeCheckpoint.shutdown();

            storeCheckpoint = new StoreCheckpoint("a/b/0000");
            assertTrue(physicMsgTimestamp == storeCheckpoint.getPhysicMsgTimestamp());
            assertTrue(logicsMsgTimestamp == storeCheckpoint.getLogicsMsgTimestamp());
        }
        catch (Throwable e) {
            e.printStackTrace();
            assertTrue(false);
        }

    }
}
