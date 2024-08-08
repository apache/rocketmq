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

package org.apache.rocketmq.broker.offset;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.collections.MapUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.RocksDBMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.ConsumeQueueStoreInterface;
import org.apache.rocketmq.store.queue.CqUnit;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.rocksdb.RocksDBException;

@RunWith(MockitoJUnitRunner.class)
public class RocksdbTransferOffsetAndCqTest {

    private final String basePath = Paths.get(System.getProperty("user.home"),
        "unit-test-store", UUID.randomUUID().toString().substring(0, 16).toUpperCase()).toString();

    private final String topic = "topic";
    private final String group = "group";
    private final String clientHost = "clientHost";
    private final int queueId = 1;

    private RocksDBConsumerOffsetManager rocksdbConsumerOffsetManager;

    private ConsumerOffsetManager consumerOffsetManager;

    private DefaultMessageStore defaultMessageStore;

    @Mock
    private BrokerController brokerController;

    @Before
    public void init() throws IOException {
        if (notToBeExecuted()) {
            return;
        }
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setConsumerOffsetUpdateVersionStep(10);
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setStorePathRootDir(basePath);
        messageStoreConfig.setTransferOffsetJsonToRocksdb(true);
        messageStoreConfig.setRocksdbCQWriteEnable(true);
        Mockito.lenient().when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        Mockito.lenient().when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);

        defaultMessageStore = new DefaultMessageStore(messageStoreConfig, new BrokerStatsManager("aaa", true), null,
            brokerConfig, new ConcurrentHashMap<String, TopicConfig>());
        defaultMessageStore.enableRocksdbCQWrite();
        defaultMessageStore.loadCheckPoint();

        consumerOffsetManager = new ConsumerOffsetManager(brokerController);
        consumerOffsetManager.load();

        rocksdbConsumerOffsetManager = new RocksDBConsumerOffsetManager(brokerController);
    }

    @Test
    public void testTransferOffset() {
        if (notToBeExecuted()) {
            return;
        }

        for (int i = 0; i < 200; i++) {
            consumerOffsetManager.commitOffset(clientHost, group, topic, queueId, i);
        }

        ConcurrentMap<String, ConcurrentMap<Integer, Long>> offsetTable = consumerOffsetManager.getOffsetTable();
        ConcurrentMap<Integer, Long> map = offsetTable.get(topic + "@" + group);
        Assert.assertTrue(MapUtils.isNotEmpty(map));

        Long offset = map.get(queueId);
        Assert.assertEquals(199L, (long) offset);

        long offsetDataVersion = consumerOffsetManager.getDataVersion().getCounter().get();
        Assert.assertEquals(20L, offsetDataVersion);

        consumerOffsetManager.persist();

        boolean loadResult = rocksdbConsumerOffsetManager.load();
        Assert.assertTrue(loadResult);

        ConcurrentMap<String, ConcurrentMap<Integer, Long>> rocksdbOffsetTable = rocksdbConsumerOffsetManager.getOffsetTable();

        ConcurrentMap<Integer, Long> rocksdbMap = rocksdbOffsetTable.get(topic + "@" + group);
        Assert.assertTrue(MapUtils.isNotEmpty(rocksdbMap));

        Long aLong1 = rocksdbMap.get(queueId);
        Assert.assertEquals(199L, (long) aLong1);

        long rocksdbOffset = rocksdbConsumerOffsetManager.getDataVersion().getCounter().get();
        Assert.assertEquals(21L, rocksdbOffset);
    }

    @Test
    public void testRocksdbCqWrite() throws RocksDBException {
        RocksDBMessageStore kvStore = defaultMessageStore.getRocksDBMessageStore();
        ConsumeQueueStoreInterface store = kvStore.getConsumeQueueStore();
        ConsumeQueueInterface rocksdbCq = defaultMessageStore.getRocksDBMessageStore().findConsumeQueue(topic, queueId);
        ConsumeQueueInterface fileCq = defaultMessageStore.findConsumeQueue(topic, queueId);
        for (int i = 0; i < 200; i++) {
            DispatchRequest request = new DispatchRequest(topic, queueId, i, 200, 0, System.currentTimeMillis(), i, "", "", 0, 0, new HashMap<>());
            fileCq.putMessagePositionInfoWrapper(request);
            store.putMessagePositionInfoWrapper(request);
        }
        Pair<CqUnit, Long> unit = rocksdbCq.getCqUnitAndStoreTime(100);
        Pair<CqUnit, Long> unit1 = fileCq.getCqUnitAndStoreTime(100);
        Assert.assertTrue(unit.getObject1().getPos() == unit1.getObject1().getPos());
    }

    private boolean notToBeExecuted() {
        return MixAll.isMac();
    }

}
