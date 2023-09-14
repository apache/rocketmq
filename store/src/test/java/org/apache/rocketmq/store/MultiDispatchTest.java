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
import java.nio.charset.Charset;

import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.rocketmq.store.config.StorePathConfigHelper.getStorePathConsumeQueue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MultiDispatchTest {

    private ConsumeQueue consumeQueue;

    private DefaultMessageStore messageStore;

    @Before
    public void init() throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMappedFileSizeCommitLog(1024 * 8);
        messageStoreConfig.setMappedFileSizeConsumeQueue(1024 * 4);
        messageStoreConfig.setMaxHashSlotNum(100);
        messageStoreConfig.setMaxIndexNum(100 * 10);
        messageStoreConfig.setStorePathRootDir(System.getProperty("java.io.tmpdir") + File.separator + "unitteststore1");
        messageStoreConfig.setStorePathCommitLog(
            System.getProperty("java.io.tmpdir") + File.separator + "unitteststore1" + File.separator + "commitlog");

        messageStoreConfig.setEnableLmq(true);
        messageStoreConfig.setEnableMultiDispatch(true);
        BrokerConfig brokerConfig = new BrokerConfig();
        //too much reference
        messageStore = new DefaultMessageStore(messageStoreConfig, null, null, brokerConfig, new ConcurrentHashMap<>());
        consumeQueue = new ConsumeQueue("xxx", 0,
            getStorePathConsumeQueue(messageStoreConfig.getStorePathRootDir()), messageStoreConfig.getMappedFileSizeConsumeQueue(), messageStore);
    }

    @After
    public void destroy() {
        UtilAll.deleteFile(new File(System.getProperty("java.io.tmpdir") + File.separator + "unitteststore1"));
    }

    @Test
    public void queueKey() {
        MessageExtBrokerInner messageExtBrokerInner = mock(MessageExtBrokerInner.class);
        when(messageExtBrokerInner.getQueueId()).thenReturn(2);
        String ret = consumeQueue.queueKey("%LMQ%lmq123", messageExtBrokerInner);
        assertEquals(ret, "%LMQ%lmq123-0");
    }

    @Test
    public void wrapMultiDispatch() {
        MessageExtBrokerInner messageExtBrokerInner = buildMessageMultiQueue();
        messageStore.assignOffset(messageExtBrokerInner);
        assertEquals(messageExtBrokerInner.getProperty(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET), "0,0");
    }

    private MessageExtBrokerInner buildMessageMultiQueue() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic("test");
        msg.setTags("TAG1");
        msg.setKeys("Hello");
        msg.setBody("aaa".getBytes(Charset.forName("UTF-8")));
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(0);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(new InetSocketAddress("127.0.0.1", 54270));
        msg.setBornHost(new InetSocketAddress("127.0.0.1", 10911));
        for (int i = 0; i < 1; i++) {
            msg.putUserProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH, "%LMQ%123,%LMQ%456");
        }
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

        return msg;
    }
}