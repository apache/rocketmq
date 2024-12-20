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

package org.apache.rocketmq.broker.config.v2;

import java.io.File;
import java.io.IOException;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.remoting.protocol.subscription.GroupRetryPolicy;
import org.apache.rocketmq.remoting.protocol.subscription.GroupRetryPolicyType;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SubscriptionGroupManagerV2Test {

    private MessageStoreConfig messageStoreConfig;

    private ConfigStorage configStorage;

    private SubscriptionGroupManagerV2 subscriptionGroupManagerV2;

    @Mock
    private BrokerController controller;

    @Mock
    private MessageStore messageStore;

    @Rule
    public TemporaryFolder tf = new TemporaryFolder();

    @After
    public void cleanUp() {
        if (null != configStorage) {
            configStorage.shutdown();
        }
    }

    @Before
    public void setUp() throws IOException {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setAutoCreateSubscriptionGroup(false);
        Mockito.doReturn(brokerConfig).when(controller).getBrokerConfig();

        Mockito.doReturn(messageStore).when(controller).getMessageStore();
        Mockito.doReturn(1L).when(messageStore).getStateMachineVersion();

        File configStoreDir = tf.newFolder();
        messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setStorePathRootDir(configStoreDir.getAbsolutePath());
        configStorage = new ConfigStorage(messageStoreConfig);
        configStorage.start();
        subscriptionGroupManagerV2 = new SubscriptionGroupManagerV2(controller, configStorage);
    }


    @Test
    public void testUpdateSubscriptionGroupConfig() {
        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName("G1");
        subscriptionGroupConfig.setConsumeEnable(true);
        subscriptionGroupConfig.setRetryMaxTimes(16);
        subscriptionGroupConfig.setGroupSysFlag(1);
        GroupRetryPolicy retryPolicy = new GroupRetryPolicy();
        retryPolicy.setType(GroupRetryPolicyType.EXPONENTIAL);
        subscriptionGroupConfig.setGroupRetryPolicy(retryPolicy);
        subscriptionGroupConfig.setBrokerId(1);
        subscriptionGroupConfig.setConsumeBroadcastEnable(true);
        subscriptionGroupConfig.setConsumeMessageOrderly(true);
        subscriptionGroupConfig.setConsumeTimeoutMinute(30);
        subscriptionGroupConfig.setConsumeFromMinEnable(true);
        subscriptionGroupConfig.setWhichBrokerWhenConsumeSlowly(1);
        subscriptionGroupConfig.setNotifyConsumerIdsChangedEnable(true);
        subscriptionGroupManagerV2.updateSubscriptionGroupConfig(subscriptionGroupConfig);

        SubscriptionGroupConfig found = subscriptionGroupManagerV2.findSubscriptionGroupConfig(subscriptionGroupConfig.getGroupName());
        Assert.assertEquals(subscriptionGroupConfig, found);

        subscriptionGroupManagerV2.getSubscriptionGroupTable().clear();
        configStorage.shutdown();

        configStorage = new ConfigStorage(messageStoreConfig);
        configStorage.start();
        subscriptionGroupManagerV2 = new SubscriptionGroupManagerV2(controller, configStorage);
        subscriptionGroupManagerV2.load();
        found = subscriptionGroupManagerV2.findSubscriptionGroupConfig(subscriptionGroupConfig.getGroupName());
        Assert.assertEquals(subscriptionGroupConfig, found);
    }


    @Test
    public void testDeleteSubscriptionGroupConfig() {
        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName("G1");
        subscriptionGroupConfig.setConsumeEnable(true);
        subscriptionGroupConfig.setRetryMaxTimes(16);
        subscriptionGroupConfig.setGroupSysFlag(1);
        GroupRetryPolicy retryPolicy = new GroupRetryPolicy();
        retryPolicy.setType(GroupRetryPolicyType.EXPONENTIAL);
        subscriptionGroupConfig.setGroupRetryPolicy(retryPolicy);
        subscriptionGroupConfig.setBrokerId(1);
        subscriptionGroupConfig.setConsumeBroadcastEnable(true);
        subscriptionGroupConfig.setConsumeMessageOrderly(true);
        subscriptionGroupConfig.setConsumeTimeoutMinute(30);
        subscriptionGroupConfig.setConsumeFromMinEnable(true);
        subscriptionGroupConfig.setWhichBrokerWhenConsumeSlowly(1);
        subscriptionGroupConfig.setNotifyConsumerIdsChangedEnable(true);
        subscriptionGroupManagerV2.updateSubscriptionGroupConfig(subscriptionGroupConfig);

        SubscriptionGroupConfig found = subscriptionGroupManagerV2.findSubscriptionGroupConfig(subscriptionGroupConfig.getGroupName());
        Assert.assertEquals(subscriptionGroupConfig, found);
        subscriptionGroupManagerV2.removeSubscriptionGroupConfig(subscriptionGroupConfig.getGroupName());

        found = subscriptionGroupManagerV2.findSubscriptionGroupConfig(subscriptionGroupConfig.getGroupName());
        Assert.assertNull(found);

        configStorage.shutdown();

        configStorage = new ConfigStorage(messageStoreConfig);
        configStorage.start();

        subscriptionGroupManagerV2 = new SubscriptionGroupManagerV2(controller, configStorage);
        subscriptionGroupManagerV2.load();
        found = subscriptionGroupManagerV2.findSubscriptionGroupConfig(subscriptionGroupConfig.getGroupName());
        Assert.assertNull(found);
    }

}
