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

package org.apache.rocketmq.store;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MultiDispatchTest {

    private CommitLog commitLog;
    private MultiDispatch multiDispatch;

    @Before
    public void init() throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMappedFileSizeCommitLog(1024 * 8);
        messageStoreConfig.setMappedFileSizeConsumeQueue(1024 * 4);
        messageStoreConfig.setMaxHashSlotNum(100);
        messageStoreConfig.setMaxIndexNum(100 * 10);
        messageStoreConfig.setStorePathRootDir(System.getProperty("user.home") + File.separator + "unitteststore1");
        messageStoreConfig.setStorePathCommitLog(
            System.getProperty("user.home") + File.separator + "unitteststore1" + File.separator + "commitlog");

        messageStoreConfig.setEnableLmq(true);
        messageStoreConfig.setEnableMultiDispatch(true);
        //too much reference
        DefaultMessageStore messageStore = new DefaultMessageStore(messageStoreConfig, null, null, null);
        this.commitLog = new CommitLog(messageStore);
        this.multiDispatch = new MultiDispatch(messageStore, commitLog);
    }

    @After
    public void destroy() {
        UtilAll.deleteFile(new File(System.getProperty("user.home") + File.separator + "unitteststore1"));
    }

    @Test
    public void queueKey() {
        MessageExtBrokerInner messageExtBrokerInner = mock(MessageExtBrokerInner.class);
        when(messageExtBrokerInner.getQueueId()).thenReturn(2);
        String ret = multiDispatch.queueKey("%LMQ%lmq123", messageExtBrokerInner);
        assertEquals(ret, "%LMQ%lmq123-0");
    }

    @Test
    public void wrapMultiDispatch() {
        MessageExtBrokerInner message = new MessageExtBrokerInner();
        message.setTopic("test");
        message.setBody("aaa".getBytes(StandardCharsets.UTF_8));
        message.setBornHost(new InetSocketAddress("127.0.0.1", 54270));
        message.setStoreHost(new InetSocketAddress("127.0.0.1", 10911));
        message.putUserProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH, "%LMQ%123,%LMQ%456");

        multiDispatch.wrapMultiDispatch(message);
        assertTrue(commitLog.getLmqTopicQueueTable().size() == 2);
        assertTrue(commitLog.getLmqTopicQueueTable().get("%LMQ%123-0") == 0L);
        assertTrue(commitLog.getLmqTopicQueueTable().get("%LMQ%456-0") == 0L);
    }

    @Test
    public void updateMultiQueueOffset() {
        MessageExtBrokerInner messageExtBrokerInner = mock(MessageExtBrokerInner.class);
        when(messageExtBrokerInner.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH)).thenReturn("%LMQ%123,%LMQ%456");
        when(messageExtBrokerInner.getProperty(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET)).thenReturn("0,1");
        multiDispatch.updateMultiQueueOffset(messageExtBrokerInner);
        assertTrue(commitLog.getLmqTopicQueueTable().size() == 2);
        assertTrue(commitLog.getLmqTopicQueueTable().get("%LMQ%123-0") == 1L);
        assertTrue(commitLog.getLmqTopicQueueTable().get("%LMQ%456-0") == 2L);
    }
}