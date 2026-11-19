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

package org.apache.rocketmq.broker.subscription;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.SubscriptionGroupAttributes;
import org.apache.rocketmq.common.attribute.Attribute;
import org.apache.rocketmq.common.attribute.BooleanAttribute;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class SubscriptionGroupManagerTest {
    private String group = "group";

    @Rule
    public TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();

    @Mock
    private BrokerController brokerControllerMock;
    private SubscriptionGroupManager subscriptionGroupManager;
    private Attribute previousTestAttribute;

    @Before
    public void before() throws Exception {
        previousTestAttribute = SubscriptionGroupAttributes.ALL.put("test", new BooleanAttribute(
            "test",
            false,
            false
        ));
        subscriptionGroupManager = spy(new SubscriptionGroupManager(brokerControllerMock));
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setStorePathRootDir(temporaryFolder.newFolder("store").getAbsolutePath());
        Mockito.lenient().when(brokerControllerMock.getMessageStoreConfig()).thenReturn(messageStoreConfig);
    }

    @After
    public void destroy() {
        try {
            if (subscriptionGroupManager != null) {
                subscriptionGroupManager.stop();
            }
        } finally {
            if (previousTestAttribute == null) {
                SubscriptionGroupAttributes.ALL.remove("test");
            } else {
                SubscriptionGroupAttributes.ALL.put("test", previousTestAttribute);
            }
        }
    }

    @Test
    public void testUpdateAndCreateSubscriptionGroupInRocksdb() {
        group += System.currentTimeMillis();
        updateSubscriptionGroupConfig();
    }

    @Test
    public void updateSubscriptionGroupConfig() {
        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName(group);
        Map<String, String> attr = ImmutableMap.of("+test", "true");
        subscriptionGroupConfig.setAttributes(attr);
        SubscriptionGroupManager subscriptionGroupManager = new SubscriptionGroupManager(brokerControllerMock);
        subscriptionGroupManager.updateSubscriptionGroupConfig(subscriptionGroupConfig);
        SubscriptionGroupConfig result = subscriptionGroupManager.getSubscriptionGroupTable().get(group);
        assertThat(result).isNotNull();
        assertThat(result.getGroupName()).isEqualTo(group);
        assertThat(result.getAttributes().get("test")).isEqualTo("true");

        SubscriptionGroupConfig subscriptionGroupConfig1 = new SubscriptionGroupConfig();
        subscriptionGroupConfig1.setGroupName(group);
        Map<String, String> attrRemove = ImmutableMap.of("-test", "");
        subscriptionGroupConfig1.setAttributes(attrRemove);
        assertThatThrownBy(() -> subscriptionGroupManager.updateSubscriptionGroupConfig(subscriptionGroupConfig1))
            .isInstanceOf(RuntimeException.class).hasMessage("attempt to update an unchangeable attribute. key: test");
    }

    @Test
    public void testUpdateSubscriptionGroupConfigList_NullConfigList() {
        subscriptionGroupManager.updateSubscriptionGroupConfigList(null);
        // Verifying that persist() is not called
        verify(subscriptionGroupManager, never()).persist();
    }

    @Test
    public void testUpdateSubscriptionGroupConfigList_EmptyConfigList() {
        subscriptionGroupManager.updateSubscriptionGroupConfigList(Collections.emptyList());
        // Verifying that persist() is not called
        verify(subscriptionGroupManager, never()).persist();
    }

    @Test
    public void testUpdateSubscriptionGroupConfigList_ValidConfigList() {
        final List<SubscriptionGroupConfig> configList = new LinkedList<>();
        final List<String> groupNames = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            SubscriptionGroupConfig config = new SubscriptionGroupConfig();
            String groupName = String.format("group-%d", i);
            config.setGroupName(groupName);
            configList.add(config);
            groupNames.add(groupName);
        }

        SubscriptionGroupManager subscriptionGroupManager = new SubscriptionGroupManager(brokerControllerMock);
        subscriptionGroupManager.updateSubscriptionGroupConfigList(configList);

        groupNames.forEach(groupName ->
            assertThat(subscriptionGroupManager.getSubscriptionGroupTable().get(groupName)).isNotNull());
    }

    @Test
    public void testDeleteSubscriptionGroupConfigWithDeferredPersist() {
        Mockito.doNothing().when(subscriptionGroupManager).persist();

        SubscriptionGroupConfig deferredGroup = new SubscriptionGroupConfig();
        deferredGroup.setGroupName("deferred-group");
        subscriptionGroupManager.getSubscriptionGroupTable().put(deferredGroup.getGroupName(), deferredGroup);
        subscriptionGroupManager.deleteSubscriptionGroupConfig(deferredGroup.getGroupName(), false);
        verify(subscriptionGroupManager, never()).persist();

        SubscriptionGroupConfig persistedGroup = new SubscriptionGroupConfig();
        persistedGroup.setGroupName("persisted-group");
        subscriptionGroupManager.getSubscriptionGroupTable().put(persistedGroup.getGroupName(), persistedGroup);
        subscriptionGroupManager.deleteSubscriptionGroupConfig(persistedGroup.getGroupName(), true);
        verify(subscriptionGroupManager).persist();
    }

    @Test
    public void testSubGroupTable() {
        // Empty SubscriptionGroupManager
        subscriptionGroupManager.getSubscriptionGroupTable().clear();
        Map<String, SubscriptionGroupConfig> result =
            subscriptionGroupManager.subGroupTable(subscriptionGroupManager.getDataVersion().toJson(), 0, 200);
        assertThat(result).isEmpty();

        // fill SubscriptionGroupManager
        int totalGroupNum = 50000;
        fillSubscriptionGroupManager(totalGroupNum);

        // Null DataVersion
        int beginIndex = 0, maxNum = 200;
        int endIndex = beginIndex + maxNum - 1;
        result = subscriptionGroupManager.subGroupTable(null, beginIndex, maxNum);

        Assert.assertEquals(maxNum, result.size());
        Assert.assertTrue(result.containsKey(String.format("group-%05d", ThreadLocalRandom.current().nextInt(beginIndex, endIndex))));
        Assert.assertFalse(result.containsKey(String.format("group-%05d", beginIndex - 1)));
        Assert.assertFalse(result.containsKey(String.format("group-%05d", endIndex + 1)));

        // Different DataVersion
        DataVersion differentVersion = new DataVersion();
        differentVersion.setCounter(new AtomicLong(1000L));  // different counter
        differentVersion.setTimestamp(System.currentTimeMillis());
        result = subscriptionGroupManager.subGroupTable(differentVersion.toJson(), 300, maxNum);

        Assert.assertEquals(maxNum, result.size());
        Assert.assertTrue(result.containsKey(String.format("group-%05d", ThreadLocalRandom.current().nextInt(beginIndex, endIndex))));
        Assert.assertFalse(result.containsKey(String.format("group-%05d", beginIndex - 1)));
        Assert.assertFalse(result.containsKey(String.format("group-%05d", endIndex + 1)));

        // BeginIndexOutOfRange
        result = subscriptionGroupManager.subGroupTable(subscriptionGroupManager.getDataVersion().toJson(), totalGroupNum, 200);

        Assert.assertTrue(result.isEmpty());

        // Normal Case
        beginIndex = 300;
        endIndex = beginIndex + maxNum - 1;
        result = subscriptionGroupManager.subGroupTable(subscriptionGroupManager.getDataVersion().toJson(), beginIndex, maxNum);

        Assert.assertEquals(maxNum, result.size());
        Assert.assertTrue(result.containsKey(String.format("group-%05d", ThreadLocalRandom.current().nextInt(beginIndex, endIndex))));
        Assert.assertFalse(result.containsKey(String.format("group-%05d", beginIndex - 1)));
        Assert.assertFalse(result.containsKey(String.format("group-%05d", endIndex + 1)));

        // NotFullTopicConfigTable
        beginIndex = 49950;
        endIndex = Math.min(subscriptionGroupManager.getSubscriptionGroupTable().size() - 1, beginIndex + maxNum - 1);
        result = subscriptionGroupManager.subGroupTable(subscriptionGroupManager.getDataVersion().toJson(), beginIndex, maxNum);

        Assert.assertEquals(totalGroupNum - beginIndex, result.size());
        Assert.assertTrue(result.containsKey(String.format("group-%05d", ThreadLocalRandom.current().nextInt(beginIndex, endIndex))));
        Assert.assertFalse(result.containsKey(String.format("group-%05d", beginIndex - 1)));
        Assert.assertFalse(result.containsKey(String.format("group-%05d", endIndex + 1)));

    }

    private void fillSubscriptionGroupManager(int num) {
        for (int i = num - 1; i >= 0; i--) {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            String groupName = String.format("group-%05d", i);
            subscriptionGroupConfig.setGroupName(groupName);
            Map<String, String> attr = ImmutableMap.of("+test", "true");
            subscriptionGroupConfig.setAttributes(attr);
            subscriptionGroupManager.getSubscriptionGroupTable().put(groupName, subscriptionGroupConfig);
        }
    }

}
