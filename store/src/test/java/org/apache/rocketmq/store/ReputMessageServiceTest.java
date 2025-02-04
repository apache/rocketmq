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

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.UUID;
import java.io.File;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageDecoder;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;

public class ReputMessageServiceTest {
    private DefaultMessageStore syncFlushMessageStore;
    private DefaultMessageStore asyncFlushMessageStore;
    private final String topic = "FooBar";
    private final String tmpdir = System.getProperty("java.io.tmpdir");
    private final String storePathRootParentDir = (StringUtils.endsWith(tmpdir, File.separator) ? tmpdir : tmpdir + File.separator) + UUID.randomUUID();
    private SocketAddress bornHost;
    private SocketAddress storeHost;

    @Before
    public void init() throws Exception {
        File file = new File(storePathRootParentDir);
        UtilAll.deleteFile(file);
        syncFlushMessageStore = buildMessageStore(FlushDiskType.SYNC_FLUSH);
        asyncFlushMessageStore = buildMessageStore(FlushDiskType.ASYNC_FLUSH);
        assertTrue(syncFlushMessageStore.load());
        assertTrue(asyncFlushMessageStore.load());
        syncFlushMessageStore.start();
        asyncFlushMessageStore.start();
        storeHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        bornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
    }

    private DefaultMessageStore buildMessageStore(FlushDiskType flushDiskType) throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setHaListenPort(0);
        messageStoreConfig.setFlushDiskType(flushDiskType);
        messageStoreConfig.setStorePathRootDir(storePathRootParentDir + File.separator + flushDiskType);
        messageStoreConfig.setStorePathCommitLog(storePathRootParentDir + File.separator + flushDiskType + File.separator + "commitlog");
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setLongPollingEnable(false);
        DefaultMessageStore messageStore = new DefaultMessageStore(messageStoreConfig, mock(BrokerStatsManager.class), null, brokerConfig, null);
        // Mock flush disk service
        Field field = CommitLog.class.getDeclaredField("flushManager");
        field.setAccessible(true);
        FlushManager flushManager = mock(FlushManager.class);
        CompletableFuture<PutMessageStatus> completableFuture = new CompletableFuture<>();
        completableFuture.complete(PutMessageStatus.PUT_OK);
        when(flushManager.handleDiskFlush(any(AppendMessageResult.class), any(MessageExt.class))).thenReturn(completableFuture);
        field.set(messageStore.getCommitLog(), flushManager);
        return messageStore;
    }

    @Test
    public void testReputEndOffset_whenSyncFlush() throws Exception {
        for (int i = 0; i < 10; i++) {
            assertEquals(PutMessageStatus.PUT_OK, syncFlushMessageStore.putMessage(buildMessage()).getPutMessageStatus());
        }
        assertEquals(1580, syncFlushMessageStore.getMaxPhyOffset());
        assertEquals(0, syncFlushMessageStore.getCommitLog().getFlushedWhere());
        // wait for cq dispatch
        Thread.sleep(3000);
        assertEquals(0, syncFlushMessageStore.getCommitLog().getFlushedWhere());
        assertEquals(0, syncFlushMessageStore.getMaxOffsetInQueue(topic, 0));
        GetMessageResult getMessageResult = syncFlushMessageStore.getMessage("testGroup", topic, 0, 0, 32, null);
        assertEquals(GetMessageStatus.NO_MESSAGE_IN_QUEUE, getMessageResult.getStatus());
    }

    @Test
    public void testReputEndOffset_whenAsyncFlush() throws Exception {
        for (int i = 0; i < 10; i++) {
            assertEquals(PutMessageStatus.PUT_OK, asyncFlushMessageStore.putMessage(buildMessage()).getPutMessageStatus());
        }
        assertEquals(1580, asyncFlushMessageStore.getMaxPhyOffset());
        assertEquals(0, asyncFlushMessageStore.getCommitLog().getFlushedWhere());
        // wait for cq dispatch
        Thread.sleep(3000);
        assertEquals(0, asyncFlushMessageStore.getCommitLog().getFlushedWhere());
        assertEquals(10, asyncFlushMessageStore.getMaxOffsetInQueue(topic, 0));
        GetMessageResult getMessageResult = asyncFlushMessageStore.getMessage("testGroup", topic, 0, 0, 32, null);
        assertEquals(10, getMessageResult.getMessageCount());
    }

    private MessageExtBrokerInner buildMessage() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(topic);
        msg.setTags("TAG1");
        msg.setBody("Once, there was a chance for me!".getBytes());
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(0);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(storeHost);
        msg.setBornHost(bornHost);
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));
        return msg;
    }

    @After
    public void destroy() throws Exception {
        if (this.syncFlushMessageStore != null) {
            syncFlushMessageStore.shutdown();
            syncFlushMessageStore.destroy();
        }
        if (this.asyncFlushMessageStore != null) {
            asyncFlushMessageStore.shutdown();
            asyncFlushMessageStore.destroy();
        }
        File file = new File(storePathRootParentDir);
        UtilAll.deleteFile(file);
    }
}
