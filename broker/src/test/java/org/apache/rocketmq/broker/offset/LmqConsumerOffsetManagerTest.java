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

import com.alibaba.fastjson.JSON;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.subscription.LmqSubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.LmqTopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Spy;

import static org.assertj.core.api.Assertions.assertThat;

public class LmqConsumerOffsetManagerTest {

    @Spy
    private BrokerController brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(),
        new NettyClientConfig(), new MessageStoreConfig());

    @Test
    public void testOffsetManage() {
        LmqConsumerOffsetManager lmqConsumerOffsetManager = new LmqConsumerOffsetManager(brokerController);
        LmqTopicConfigManager lmqTopicConfigManager = new LmqTopicConfigManager(brokerController);
        LmqSubscriptionGroupManager lmqSubscriptionGroupManager = new LmqSubscriptionGroupManager(brokerController);

        String lmqTopicName = "%LMQ%1111";
        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName(lmqTopicName);
        lmqTopicConfigManager.updateTopicConfig(topicConfig);
        TopicConfig topicConfig1 = lmqTopicConfigManager.selectTopicConfig(lmqTopicName);
        assertThat(topicConfig1.getTopicName()).isEqualTo(topicConfig.getTopicName());

        String lmqGroupName = "%LMQ%GID_test";
        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName(lmqGroupName);
        lmqSubscriptionGroupManager.updateSubscriptionGroupConfig(subscriptionGroupConfig);
        SubscriptionGroupConfig subscriptionGroupConfig1 = lmqSubscriptionGroupManager.findSubscriptionGroupConfig(
            lmqGroupName);
        assertThat(subscriptionGroupConfig1.getGroupName()).isEqualTo(subscriptionGroupConfig.getGroupName());

        lmqConsumerOffsetManager.commitOffset("127.0.0.1", lmqGroupName, lmqTopicName, 0, 10L);
        Map<Integer, Long> integerLongMap = lmqConsumerOffsetManager.queryOffset(lmqGroupName, lmqTopicName);
        assertThat(integerLongMap.get(0)).isEqualTo(10L);
        long offset = lmqConsumerOffsetManager.queryOffset(lmqGroupName, lmqTopicName, 0);
        assertThat(offset).isEqualTo(10L);

        long offset1 = lmqConsumerOffsetManager.queryOffset(lmqGroupName, lmqTopicName + "test", 0);
        assertThat(offset1).isEqualTo(-1L);
    }

    @Test
    public void testOffsetManage1() {
        LmqConsumerOffsetManager lmqConsumerOffsetManager = new LmqConsumerOffsetManager(brokerController);

        String lmqTopicName = "%LMQ%1111";

        String lmqGroupName = "%LMQ%GID_test";

        lmqConsumerOffsetManager.commitOffset("127.0.0.1", lmqGroupName, lmqTopicName, 0, 10L);

        lmqTopicName = "%LMQ%1222";

        lmqGroupName = "%LMQ%GID_test222";

        lmqConsumerOffsetManager.commitOffset("127.0.0.1", lmqGroupName, lmqTopicName, 0, 10L);
        lmqConsumerOffsetManager.commitOffset("127.0.0.1","GID_test1", "MqttTest",0, 10L);

        String json = lmqConsumerOffsetManager.encode(true);

        LmqConsumerOffsetManager lmqConsumerOffsetManager1 = new LmqConsumerOffsetManager(brokerController);

        lmqConsumerOffsetManager1.decode(json);

        assertThat(lmqConsumerOffsetManager1.getOffsetTable().size()).isEqualTo(1);
        assertThat(lmqConsumerOffsetManager1.getLmqOffsetTable().size()).isEqualTo(2);
    }

    @Test
    public void testUpgradeCompatible() throws IOException, InterruptedException {
        //load old consumerOffset
        ConsumerOffsetManager consumerOffsetManager = new ConsumerOffsetManager(brokerController);
        consumerOffsetManager.commitOffset("127.0.0.1","GID_test2", "OldTopic",0, 11L);
        String json = JSON.toJSONString(consumerOffsetManager);
        String configFilePath = consumerOffsetManager.configFilePath();

        persistOffsetFile(configFilePath, json);
        ConcurrentMap<String, ConcurrentMap<Integer, Long>> oldOffsetTable = consumerOffsetManager.getOffsetTable();
        String oldOffset = JSON.toJSONString(oldOffsetTable);

        //[UPGRADE]init lmq consumer offset manager
        LmqConsumerOffsetManager lmqConsumerOffsetManager = new LmqConsumerOffsetManager(brokerController);
        lmqConsumerOffsetManager.load();
        ConcurrentMap<String, ConcurrentMap<Integer, Long>> offsetTable = lmqConsumerOffsetManager.getOffsetTable();
        String newOffsetJson = JSON.toJSONString(offsetTable);
        Assert.assertEquals(oldOffset, newOffsetJson);
    }

    @Test
    public void testRollBack() throws IOException, InterruptedException {
        //generate lmq offset
        LmqConsumerOffsetManager lmqConsumerOffsetManager = new LmqConsumerOffsetManager(brokerController);
        lmqConsumerOffsetManager.commitOffset("127.0.0.1", "GID_test1", "OldTopic", 1, 12L);
        String json = JSON.toJSONString(lmqConsumerOffsetManager);
        String configFilePath = lmqConsumerOffsetManager.configFilePath();
        persistOffsetFile(configFilePath, json);
        Thread.sleep(1000);

        //init consumerOffsetManager
        ConsumerOffsetManager consumerOffsetManager = new ConsumerOffsetManager(brokerController);
        consumerOffsetManager.commitOffset("127.0.0.1", "GID_test2", "NewTopic", 0, 12L);
        String offsetJson = JSON.toJSONString(consumerOffsetManager);
        String filePath = consumerOffsetManager.configFilePath();
        persistOffsetFile(filePath, offsetJson);
        String offsetTableOld = JSON.toJSONString(consumerOffsetManager.getOffsetTable());

        //clear old offset info
        consumerOffsetManager.getOffsetTable().clear();
        Assert.assertEquals(0, consumerOffsetManager.getOffsetTable().size());
        consumerOffsetManager.load();
        Assert.assertEquals(1, consumerOffsetManager.getOffsetTable().size());

        String offsetTableNew = JSON.toJSONString(consumerOffsetManager.getOffsetTable());
        Assert.assertEquals(offsetTableOld, offsetTableNew);
        Thread.sleep(1000);

        //roll back and lmqOffset is modified resently
        lmqConsumerOffsetManager.commitOffset("127.0.0.1", "GID_test2", "OldTopic", 0, 10L);
        String newLmqOffsetJson = JSON.toJSONString(lmqConsumerOffsetManager);
        persistOffsetFile(configFilePath, newLmqOffsetJson);
        String expectOffset = JSON.toJSONString(lmqConsumerOffsetManager.getOffsetTable());

        //clear old offset info
        consumerOffsetManager.getOffsetTable().clear();
        Assert.assertEquals(0, consumerOffsetManager.getOffsetTable().size());
        consumerOffsetManager.load();
        Assert.assertEquals(2, consumerOffsetManager.getOffsetTable().size());
        String actualOffset = JSON.toJSONString(consumerOffsetManager.getOffsetTable());
        Assert.assertEquals(expectOffset, actualOffset);
    }

    @Test
    public void testUpgradeWithOldAndNewFile() throws IOException {
        //generate normal offset table
        ConsumerOffsetManager consumerOffsetManager = new ConsumerOffsetManager(brokerController);
        consumerOffsetManager.commitOffset("127.0.0.1", "GID_test3", "NormalTopic", 0, 10L);
        String consumerOffsetManagerJson = JSON.toJSONString(consumerOffsetManager);
        String consumerOffsetConfigPath = consumerOffsetManager.configFilePath();
        persistOffsetFile(consumerOffsetConfigPath, consumerOffsetManagerJson);

        //generate old lmq offset table
        LmqConsumerOffsetManager lmqConsumerOffsetManager = new LmqConsumerOffsetManager(brokerController);
        lmqConsumerOffsetManager.commitOffset("127.0.0.1", "GID_test1", "OldTopic", 0, 10L);
        lmqConsumerOffsetManager.commitOffset("127.0.0.1", "GID_test2", "OldTopic", 0, 10L);
        String expectLmqOffsetJson = JSON.toJSONString(lmqConsumerOffsetManager.getOffsetTable());
        String lmqConfigPath = lmqConsumerOffsetManager.configFilePath();
        String lmqManagerJson = JSON.toJSONString(lmqConsumerOffsetManager);
        persistOffsetFile(lmqConfigPath, lmqManagerJson);

        //reset lmq offset table
        lmqConsumerOffsetManager.getOffsetTable().clear();
        Assert.assertEquals(0, lmqConsumerOffsetManager.getOffsetTable().size());
        lmqConsumerOffsetManager.load();
        Assert.assertEquals(2, lmqConsumerOffsetManager.getOffsetTable().size());
        String actualLmqOffsetJson = JSON.toJSONString(lmqConsumerOffsetManager.getOffsetTable());
        Assert.assertEquals(expectLmqOffsetJson, actualLmqOffsetJson);
    }

    @Test
    public void testLoadBakCompatible() throws IOException, InterruptedException {
        //generate normal offset table
        ConsumerOffsetManager consumerOffsetManager = new ConsumerOffsetManager(brokerController);
        consumerOffsetManager.commitOffset("127.0.0.1", "GID_test3", "NormalTopic", 0, 10L);
        String consumerOffsetManagerJson = JSON.toJSONString(consumerOffsetManager);
        String consumerOffsetConfigPath = consumerOffsetManager.configFilePath() + ".bak";

        //generate old lmq offset table
        LmqConsumerOffsetManager lmqConsumerOffsetManager = new LmqConsumerOffsetManager(brokerController);
        lmqConsumerOffsetManager.commitOffset("127.0.0.1", "GID_test1", "OldTopic", 0, 10L);
        lmqConsumerOffsetManager.commitOffset("127.0.0.1", "GID_test2", "OldTopic", 0, 10L);
        lmqConsumerOffsetManager.commitOffset("127.0.0.1", "%LMQ%GID_test222", "%LMQ%1222", 0, 10L);
        String lmqConfigPath = lmqConsumerOffsetManager.configFilePath() + ".bak";
        String lmqManagerJson = JSON.toJSONString(lmqConsumerOffsetManager);
        persistOffsetFile(lmqConfigPath, lmqManagerJson);
        Thread.sleep(1000);
        persistOffsetFile(consumerOffsetConfigPath, consumerOffsetManagerJson);
        String expectLmqOffsetJson = JSON.toJSONString(consumerOffsetManager.getOffsetTable());

        //reset lmq offset table
        lmqConsumerOffsetManager.getOffsetTable().clear();
        lmqConsumerOffsetManager.getLmqOffsetTable().clear();
        Assert.assertEquals(0, lmqConsumerOffsetManager.getOffsetTable().size());
        lmqConsumerOffsetManager.loadBak();
        Assert.assertEquals(1, lmqConsumerOffsetManager.getOffsetTable().size());
        Assert.assertEquals(1, lmqConsumerOffsetManager.getLmqOffsetTable().size());
        String actualLmqOffsetJson = JSON.toJSONString(lmqConsumerOffsetManager.getOffsetTable());
        Assert.assertEquals(expectLmqOffsetJson, actualLmqOffsetJson);
    }

    public void persistOffsetFile(String filePath, String detail) throws IOException {
        File file = new File(filePath);
        boolean mkdirs = file.getParentFile().mkdirs();
        FileWriter writer = new FileWriter(file);
        writer.write(detail);
        writer.close();
    }


    @After
    public void destroy() {
        UtilAll.deleteFile(new File(new MessageStoreConfig().getStorePathRootDir()));
    }

}
