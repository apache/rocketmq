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

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.rocksdb.RocksDBException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.Silent.class)
public class HAServerTest {
    private DefaultMessageStore defaultMessageStore;
    private MessageStoreConfig storeConfig;
    private HAService haService;
    private Random random = new Random();
    private List<HAClient> haClientList = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        this.storeConfig = new MessageStoreConfig();
        this.storeConfig.setHaListenPort(9000 + random.nextInt(1000));
        this.storeConfig.setHaSendHeartbeatInterval(10);

        this.defaultMessageStore = mockMessageStore();
        this.haService = new DefaultHAService();
        this.haService.init(defaultMessageStore);
        this.haService.start();
    }

    @After
    public void tearDown() {
        tearDownAllHAClient();

        await().atMost(Duration.ofMinutes(1)).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return HAServerTest.this.haService.getConnectionCount().get() == 0;
            }
        });

        this.haService.shutdown();
    }

    @Test
    public void testConnectionList_OneHAClient() throws IOException {
        setUpOneHAClient();

        await().atMost(Duration.ofMinutes(1)).until(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return HAServerTest.this.haService.getConnectionCount().get() == 1;
            }
        });
    }

    @Test
    public void testConnectionList_MultipleHAClient() throws IOException {
        setUpOneHAClient();
        setUpOneHAClient();
        setUpOneHAClient();

        await().atMost(Duration.ofMinutes(1)).until(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return HAServerTest.this.haService.getConnectionCount().get() == 3;
            }
        });

        tearDownOneHAClient();

        await().atMost(Duration.ofMinutes(1)).until(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return HAServerTest.this.haService.getConnectionCount().get() == 2;
            }
        });
    }

    @Test
    public void inSyncReplicasNums() throws IOException, RocksDBException {
        DefaultMessageStore messageStore = mockMessageStore();
        doReturn(123L).when(messageStore).getMaxPhyOffset();
        doReturn(123L).when(messageStore).getMasterFlushedOffset();
        setUpOneHAClient(messageStore);

        messageStore = mockMessageStore();
        doReturn(124L).when(messageStore).getMaxPhyOffset();
        doReturn(124L).when(messageStore).getMasterFlushedOffset();
        setUpOneHAClient(messageStore);

        messageStore = mockMessageStore();
        doReturn(123L).when(messageStore).getMaxPhyOffset();
        doReturn(123L).when(messageStore).getMasterFlushedOffset();
        setUpOneHAClient(messageStore);

        messageStore = mockMessageStore();
        doReturn(125L).when(messageStore).getMaxPhyOffset();
        doReturn(125L).when(messageStore).getMasterFlushedOffset();
        setUpOneHAClient(messageStore);

        final int haSlaveFallbehindMax = this.defaultMessageStore.getMessageStoreConfig().getHaMaxGapNotInSync();

        await().atMost(Duration.ofMinutes(1)).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return HAServerTest.this.haService.inSyncReplicasNums(haSlaveFallbehindMax) == 5;
            }
        });

        assertThat(HAServerTest.this.haService.inSyncReplicasNums(123L + haSlaveFallbehindMax)).isEqualTo(3);
        assertThat(HAServerTest.this.haService.inSyncReplicasNums(124L + haSlaveFallbehindMax)).isEqualTo(2);
        assertThat(HAServerTest.this.haService.inSyncReplicasNums(125L + haSlaveFallbehindMax)).isEqualTo(1);
    }

    @Test
    public void isSlaveOK() throws IOException, RocksDBException {
        DefaultMessageStore messageStore = mockMessageStore();
        doReturn(123L).when(messageStore).getMaxPhyOffset();
        doReturn(123L).when(messageStore).getMasterFlushedOffset();
        setUpOneHAClient(messageStore);

        messageStore = mockMessageStore();
        doReturn(124L).when(messageStore).getMaxPhyOffset();
        doReturn(124L).when(messageStore).getMasterFlushedOffset();
        setUpOneHAClient(messageStore);

        final int haSlaveFallbehindMax = this.defaultMessageStore.getMessageStoreConfig().getHaMaxGapNotInSync();

        await().atMost(Duration.ofMinutes(1)).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return HAServerTest.this.haService.isSlaveOK(haSlaveFallbehindMax + 123);
            }
        });

        assertThat(HAServerTest.this.haService.isSlaveOK(122L + haSlaveFallbehindMax)).isTrue();
        assertThat(HAServerTest.this.haService.isSlaveOK(124L + haSlaveFallbehindMax)).isFalse();
    }

    @Test
    public void putRequest_SingleAck()
        throws IOException, ExecutionException, InterruptedException, TimeoutException, RocksDBException {
        CommitLog.GroupCommitRequest request = new CommitLog.GroupCommitRequest(124, 4000, 1);
        this.haService.putRequest(request);

        assertThat(request.future().get()).isEqualTo(PutMessageStatus.FLUSH_SLAVE_TIMEOUT);

        DefaultMessageStore messageStore = mockMessageStore();
        doReturn(124L).when(messageStore).getMaxPhyOffset();
        doReturn(124L).when(messageStore).getMasterFlushedOffset();
        setUpOneHAClient(messageStore);

        request = new CommitLog.GroupCommitRequest(124, 4000, 1);
        this.haService.putRequest(request);
        assertThat(request.future().get()).isEqualTo(PutMessageStatus.PUT_OK);
    }

    @Test
    public void putRequest_MultipleAckAndRequests()
        throws IOException, ExecutionException, InterruptedException, RocksDBException {
        CommitLog.GroupCommitRequest oneAck = new CommitLog.GroupCommitRequest(124, 4000, 2);
        this.haService.putRequest(oneAck);

        CommitLog.GroupCommitRequest twoAck = new CommitLog.GroupCommitRequest(124, 4000, 3);
        this.haService.putRequest(twoAck);

        DefaultMessageStore messageStore = mockMessageStore();
        doReturn(125L).when(messageStore).getMaxPhyOffset();
        doReturn(125L).when(messageStore).getMasterFlushedOffset();
        setUpOneHAClient(messageStore);

        assertThat(oneAck.future().get()).isEqualTo(PutMessageStatus.PUT_OK);
        assertThat(twoAck.future().get()).isEqualTo(PutMessageStatus.FLUSH_SLAVE_TIMEOUT);

        messageStore = mockMessageStore();
        doReturn(128L).when(messageStore).getMaxPhyOffset();
        doReturn(128L).when(messageStore).getMasterFlushedOffset();
        setUpOneHAClient(messageStore);

        twoAck = new CommitLog.GroupCommitRequest(124, 4000, 3);
        this.haService.putRequest(twoAck);
        assertThat(twoAck.future().get()).isEqualTo(PutMessageStatus.PUT_OK);
    }

    @Test
    public void getPush2SlaveMaxOffset() throws IOException, RocksDBException {
        DefaultMessageStore messageStore = mockMessageStore();
        doReturn(123L).when(messageStore).getMaxPhyOffset();
        doReturn(123L).when(messageStore).getMasterFlushedOffset();
        setUpOneHAClient(messageStore);

        messageStore = mockMessageStore();
        doReturn(124L).when(messageStore).getMaxPhyOffset();
        doReturn(124L).when(messageStore).getMasterFlushedOffset();
        setUpOneHAClient(messageStore);

        messageStore = mockMessageStore();
        doReturn(125L).when(messageStore).getMaxPhyOffset();
        doReturn(125L).when(messageStore).getMasterFlushedOffset();
        setUpOneHAClient(messageStore);

        await().atMost(Duration.ofMinutes(1)).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return HAServerTest.this.haService.getPush2SlaveMaxOffset().get() == 125L;
            }
        });
    }

    private void setUpOneHAClient(DefaultMessageStore defaultMessageStore) throws IOException {
        HAClient haClient = new DefaultHAClient(defaultMessageStore);
        haClient.updateHaMasterAddress("127.0.0.1:" + this.storeConfig.getHaListenPort());
        haClient.start();
        this.haClientList.add(haClient);
    }

    private void setUpOneHAClient() throws IOException {
        HAClient haClient = new DefaultHAClient(this.defaultMessageStore);
        haClient.updateHaMasterAddress("127.0.0.1:" + this.storeConfig.getHaListenPort());
        haClient.start();
        this.haClientList.add(haClient);
    }

    private DefaultMessageStore mockMessageStore() throws IOException, RocksDBException {
        DefaultMessageStore messageStore = mock(DefaultMessageStore.class);
        BrokerConfig brokerConfig = mock(BrokerConfig.class);

        doReturn(true).when(brokerConfig).isInBrokerContainer();
        doReturn("mock").when(brokerConfig).getIdentifier();
        doReturn(brokerConfig).when(messageStore).getBrokerConfig();
        doReturn(new SystemClock()).when(messageStore).getSystemClock();
        doAnswer(invocation -> System.currentTimeMillis()).when(messageStore).now();
        doReturn(this.storeConfig).when(messageStore).getMessageStoreConfig();
        doReturn(new BrokerConfig()).when(messageStore).getBrokerConfig();
        doReturn(true).when(messageStore).isOffsetAligned(anyLong());
//        doReturn(new PutMessageResult(PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK))).when(messageStore).sendMsgBack(anyLong());
        doReturn(true).when(messageStore).truncateFiles(anyLong());

        DefaultMessageStore masterStore = mock(DefaultMessageStore.class);
        doReturn(Long.MAX_VALUE).when(masterStore).getFlushedWhere();
        doReturn(masterStore).when(messageStore).getMasterStoreInProcess();

        CommitLog commitLog = new CommitLog(messageStore);
        doReturn(commitLog).when(messageStore).getCommitLog();
        return messageStore;
    }

    private void tearDownOneHAClient() {
        final HAClient haClient = this.haClientList.remove(0);
        haClient.shutdown();
    }

    private void tearDownAllHAClient() {
        for (final HAClient client : this.haClientList) {
            client.shutdown();
        }
        this.haClientList.clear();
    }
}
