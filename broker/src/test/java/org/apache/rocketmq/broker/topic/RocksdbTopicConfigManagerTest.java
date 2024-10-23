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
package org.apache.rocketmq.broker.topic;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.config.v1.RocksDBTopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicAttributes;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.Attribute;
import org.apache.rocketmq.common.attribute.BooleanAttribute;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.attribute.EnumAttribute;
import org.apache.rocketmq.common.attribute.LongRangeAttribute;
import org.apache.rocketmq.common.utils.QueueTypeUtils;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RocksdbTopicConfigManagerTest {

    private final String basePath = Paths.get(System.getProperty("user.home"),
            "unit-test-store", UUID.randomUUID().toString().substring(0, 16).toUpperCase()).toString();

    private RocksDBTopicConfigManager topicConfigManager;
    @Mock
    private BrokerController brokerController;

    @Mock
    private DefaultMessageStore defaultMessageStore;

    @Before
    public void init() {
        if (notToBeExecuted()) {
            return;
        }
        BrokerConfig brokerConfig = new BrokerConfig();
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setStorePathRootDir(basePath);
        messageStoreConfig.setTransferMetadataJsonToRocksdb(true);
        when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        Mockito.lenient().when(brokerController.getMessageStore()).thenReturn(defaultMessageStore);
        Mockito.lenient().when(defaultMessageStore.getStateMachineVersion()).thenReturn(0L);
        topicConfigManager = new RocksDBTopicConfigManager(brokerController);
        topicConfigManager.load();
    }

    @After
    public void destroy() {
        if (notToBeExecuted()) {
            return;
        }
        if (topicConfigManager != null) {
            topicConfigManager.stop();
        }
    }

    @Test
    public void testAddUnsupportedKeyOnCreating() {
        if (notToBeExecuted()) {
            return;
        }
        String unsupportedKey = "key4";
        String topicName = "testAddUnsupportedKeyOnCreating-" + System.currentTimeMillis();

        supportAttributes(asList(
            new EnumAttribute("enum.key", true, newHashSet("enum-1", "enum-2", "enum-3"), "enum-1"),
            new BooleanAttribute("bool.key", false, false),
            new LongRangeAttribute("long.range.key", true, 10, 20, 15)
        ));

        Map<String, String> attributes = new HashMap<>();
        attributes.put("+enum.key", "enum-2");
        attributes.put("+" + unsupportedKey, "value1");

        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName(topicName);
        topicConfig.setAttributes(attributes);

        RuntimeException runtimeException = Assert.assertThrows(RuntimeException.class, () -> topicConfigManager.updateTopicConfig(topicConfig));
        Assert.assertEquals("unsupported key: " + unsupportedKey, runtimeException.getMessage());
    }

    @Test
    public void testAddWrongFormatKeyOnCreating() {
        if (notToBeExecuted()) {
            return;
        }
        String topicName = "testAddWrongFormatKeyOnCreating-" + System.currentTimeMillis();

        supportAttributes(asList(
            new EnumAttribute("enum.key", true, newHashSet("enum-1", "enum-2", "enum-3"), "enum-1"),
            new BooleanAttribute("bool.key", false, false),
            new LongRangeAttribute("long.range.key", true, 10, 20, 15)
        ));

        Map<String, String> attributes = new HashMap<>();
        attributes.put("++enum.key", "value1");

        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName(topicName);
        topicConfig.setAttributes(attributes);

        RuntimeException runtimeException = Assert.assertThrows(RuntimeException.class, () -> topicConfigManager.updateTopicConfig(topicConfig));
        Assert.assertEquals("kv string format wrong.", runtimeException.getMessage());
    }

    @Test
    public void testDeleteKeyOnCreating() {
        if (notToBeExecuted()) {
            return;
        }
        String topicName = "testDeleteKeyOnCreating-" + System.currentTimeMillis();

        String key = "enum.key";
        supportAttributes(asList(
            new EnumAttribute("enum.key", true, newHashSet("enum-1", "enum-2", "enum-3"), "enum-1"),
            new BooleanAttribute("bool.key", false, false),
            new LongRangeAttribute("long.range.key", true, 10, 20, 15)
        ));

        Map<String, String> attributes = new HashMap<>();
        attributes.put("-" + key, "");

        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName(topicName);
        topicConfig.setAttributes(attributes);

        RuntimeException runtimeException = Assert.assertThrows(RuntimeException.class, () -> topicConfigManager.updateTopicConfig(topicConfig));
        Assert.assertEquals("only add attribute is supported while creating topic. key: " + key, runtimeException.getMessage());
    }

    @Test
    public void testAddWrongValueOnCreating() {
        if (notToBeExecuted()) {
            return;
        }
        String topicName = "testAddWrongValueOnCreating-" + System.currentTimeMillis();

        Map<String, String> attributes = new HashMap<>();
        attributes.put("+" + TopicAttributes.QUEUE_TYPE_ATTRIBUTE.getName(), "wrong-value");

        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName(topicName);
        topicConfig.setAttributes(attributes);

        RuntimeException runtimeException = Assert.assertThrows(RuntimeException.class, () -> topicConfigManager.updateTopicConfig(topicConfig));
        Assert.assertEquals("value is not in set: [SimpleCQ, BatchCQ]", runtimeException.getMessage());
    }

    @Test
    public void testNormalAddKeyOnCreating() {
        if (notToBeExecuted()) {
            return;
        }
        String topic = "testNormalAddKeyOnCreating-" + System.currentTimeMillis();

        supportAttributes(asList(
            new EnumAttribute("enum.key", true, newHashSet("enum-1", "enum-2", "enum-3"), "enum-1"),
            new BooleanAttribute("bool.key", false, false),
            new LongRangeAttribute("long.range.key", true, 10, 20, 15)
        ));

        Map<String, String> attributes = new HashMap<>();
        attributes.put("+enum.key", "enum-2");
        attributes.put("+long.range.key", "16");

        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName(topic);
        topicConfig.setAttributes(attributes);
        topicConfigManager.updateTopicConfig(topicConfig);

        TopicConfig existingTopicConfig = topicConfigManager.getTopicConfigTable().get(topic);
        Assert.assertEquals("enum-2", existingTopicConfig.getAttributes().get("enum.key"));
        Assert.assertEquals("16", existingTopicConfig.getAttributes().get("long.range.key"));
    }

    @Test
    public void testAddDuplicatedKeyOnUpdating() {
        if (notToBeExecuted()) {
            return;
        }
        String duplicatedKey = "long.range.key";
        String topicName = "testAddDuplicatedKeyOnUpdating-" + System.currentTimeMillis();

        supportAttributes(asList(
            new EnumAttribute("enum.key", true, newHashSet("enum-1", "enum-2", "enum-3"), "enum-1"),
            new BooleanAttribute("bool.key", false, false),
            new LongRangeAttribute("long.range.key", true, 10, 20, 15)
        ));

        Map<String, String> attributes = new HashMap<>();
        attributes.put("+enum.key", "enum-3");
        attributes.put("+bool.key", "true");
        attributes.put("+long.range.key", "12");
        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName(topicName);
        topicConfig.setAttributes(attributes);
        topicConfigManager.updateTopicConfig(topicConfig);



        attributes = new HashMap<>();
        attributes.put("+" + duplicatedKey, "11");
        attributes.put("-" + duplicatedKey, "");
        TopicConfig duplicateTopicConfig = new TopicConfig();
        duplicateTopicConfig.setTopicName(topicName);
        duplicateTopicConfig.setAttributes(attributes);

        RuntimeException runtimeException = Assert.assertThrows(RuntimeException.class, () -> topicConfigManager.updateTopicConfig(duplicateTopicConfig));
        Assert.assertEquals("alter duplication key. key: " + duplicatedKey, runtimeException.getMessage());
    }

    @Test
    public void testDeleteNonexistentKeyOnUpdating() {
        if (notToBeExecuted()) {
            return;
        }
        String key = "nonexisting.key";
        String topicName = "testDeleteNonexistentKeyOnUpdating-" + System.currentTimeMillis();

        supportAttributes(asList(
            new EnumAttribute("enum.key", true, newHashSet("enum-1", "enum-2", "enum-3"), "enum-1"),
            new BooleanAttribute("bool.key", false, false),
            new LongRangeAttribute("long.range.key", true, 10, 20, 15)
        ));

        Map<String, String> attributes = new HashMap<>();
        attributes.put("+enum.key", "enum-2");
        attributes.put("+bool.key", "true");

        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName(topicName);
        topicConfig.setAttributes(attributes);

        topicConfigManager.updateTopicConfig(topicConfig);

        attributes = new HashMap<>();
        attributes.clear();
        attributes.put("-" + key, "");
        topicConfig.setAttributes(attributes);
        RuntimeException runtimeException = Assert.assertThrows(RuntimeException.class, () -> topicConfigManager.updateTopicConfig(topicConfig));
        Assert.assertEquals("attempt to delete a nonexistent key: " + key, runtimeException.getMessage());
    }

    @Test
    public void testAlterTopicWithoutChangingAttributes() {
        if (notToBeExecuted()) {
            return;
        }
        String topic = "testAlterTopicWithoutChangingAttributes-" + System.currentTimeMillis();

        supportAttributes(asList(
            new EnumAttribute("enum.key", true, newHashSet("enum-1", "enum-2", "enum-3"), "enum-1"),
            new BooleanAttribute("bool.key", false, false),
            new LongRangeAttribute("long.range.key", true, 10, 20, 15)
        ));

        Map<String, String> attributes = new HashMap<>();
        attributes.put("+enum.key", "enum-2");
        attributes.put("+bool.key", "true");

        TopicConfig topicConfigInit = new TopicConfig();
        topicConfigInit.setTopicName(topic);
        topicConfigInit.setAttributes(attributes);

        topicConfigManager.updateTopicConfig(topicConfigInit);
        Assert.assertEquals("enum-2", topicConfigManager.getTopicConfigTable().get(topic).getAttributes().get("enum.key"));
        Assert.assertEquals("true", topicConfigManager.getTopicConfigTable().get(topic).getAttributes().get("bool.key"));

        TopicConfig topicConfigAlter = new TopicConfig();
        topicConfigAlter.setTopicName(topic);
        topicConfigAlter.setReadQueueNums(10);
        topicConfigAlter.setWriteQueueNums(10);
        topicConfigManager.updateTopicConfig(topicConfigAlter);
        Assert.assertEquals("enum-2", topicConfigManager.getTopicConfigTable().get(topic).getAttributes().get("enum.key"));
        Assert.assertEquals("true", topicConfigManager.getTopicConfigTable().get(topic).getAttributes().get("bool.key"));
    }

    @Test
    public void testNormalUpdateUnchangeableKeyOnUpdating() {
        if (notToBeExecuted()) {
            return;
        }
        String topic = "testNormalUpdateUnchangeableKeyOnUpdating-" + System.currentTimeMillis();

        supportAttributes(asList(
            new EnumAttribute("enum.key", true, newHashSet("enum-1", "enum-2", "enum-3"), "enum-1"),
            new BooleanAttribute("bool.key", true, false),
            new LongRangeAttribute("long.range.key", false, 10, 20, 15)
        ));

        Map<String, String> attributes = new HashMap<>();
        attributes.put("+long.range.key", "14");

        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName(topic);
        topicConfig.setAttributes(attributes);

        topicConfigManager.updateTopicConfig(topicConfig);

        attributes.put("+long.range.key", "16");
        topicConfig.setAttributes(attributes);
        RuntimeException runtimeException = Assert.assertThrows(RuntimeException.class, () -> topicConfigManager.updateTopicConfig(topicConfig));
        Assert.assertEquals("attempt to update an unchangeable attribute. key: long.range.key", runtimeException.getMessage());
    }

    @Test
    public void testNormalQueryKeyOnGetting() {
        if (notToBeExecuted()) {
            return;
        }
        String topic = "testNormalQueryKeyOnGetting-" + System.currentTimeMillis();
        String unchangeable = "bool.key";

        supportAttributes(asList(
            new EnumAttribute("enum.key", true, newHashSet("enum-1", "enum-2", "enum-3"), "enum-1"),
            new BooleanAttribute("bool.key", false, false),
            new LongRangeAttribute("long.range.key", true, 10, 20, 15)
        ));

        Map<String, String> attributes = new HashMap<>();
        attributes.put("+" + unchangeable, "true");

        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName(topic);
        topicConfig.setAttributes(attributes);

        topicConfigManager.updateTopicConfig(topicConfig);

        TopicConfig topicConfigUpdated = topicConfigManager.getTopicConfigTable().get(topic);
        Assert.assertEquals(CQType.SimpleCQ, QueueTypeUtils.getCQType(Optional.of(topicConfigUpdated)));

        Assert.assertEquals("true", topicConfigUpdated.getAttributes().get(unchangeable));
    }

    private void supportAttributes(List<Attribute> supportAttributes) {
        Map<String, Attribute> supportedAttributes = new HashMap<>();

        for (Attribute supportAttribute : supportAttributes) {
            supportedAttributes.put(supportAttribute.getName(), supportAttribute);
        }

        TopicAttributes.ALL.putAll(supportedAttributes);
    }

    private boolean notToBeExecuted() {
        return MixAll.isMac();
    }
}
