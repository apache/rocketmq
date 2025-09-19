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

package org.apache.rocketmq.broker.metrics;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.metrics.MetricsExporterType;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class BrokerMetricsManagerTest {

    private BrokerMetricsManager createTestBrokerMetricsManager() {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        String storePathRootDir = System.getProperty("java.io.tmpdir") + File.separator + "store-"
                + UUID.randomUUID();
        messageStoreConfig.setStorePathRootDir(storePathRootDir);
        BrokerConfig brokerConfig = new BrokerConfig();

        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(0);

        BrokerController brokerController = new BrokerController(brokerConfig, nettyServerConfig,
                new NettyClientConfig(), messageStoreConfig);

        return new BrokerMetricsManager(brokerController);
    }

    @Test
    public void testNewAttributesBuilder() {
        BrokerMetricsManager metricsManager = createTestBrokerMetricsManager();
        Attributes attributes = metricsManager.newAttributesBuilder().put("a", "b")
                .build();
        assertThat(attributes.get(AttributeKey.stringKey("a"))).isEqualTo("b");
    }

    @Test
    public void testCustomizedAttributesBuilder() {
        BrokerMetricsManager metricsManager = createTestBrokerMetricsManager();
        
        // Create a custom attributes builder supplier for testing
        metricsManager.setAttributesBuilderSupplier(() -> new AttributesBuilder() {
            private AttributesBuilder attributesBuilder = Attributes.builder();

            @Override
            public Attributes build() {
                return attributesBuilder.put("customized", "value").build();
            }

            @Override
            public <T> AttributesBuilder put(AttributeKey<Long> key, int value) {
                attributesBuilder.put(key, value);
                return this;
            }

            @Override
            public <T> AttributesBuilder put(AttributeKey<T> key, T value) {
                attributesBuilder.put(key, value);
                return this;
            }

            @Override
            public AttributesBuilder putAll(Attributes attributes) {
                attributesBuilder.putAll(attributes);
                return this;
            }
        });
        
        Attributes attributes = metricsManager.newAttributesBuilder().put("a", "b")
                .build();
        assertThat(attributes.get(AttributeKey.stringKey("a"))).isEqualTo("b");
        assertThat(attributes.get(AttributeKey.stringKey("customized"))).isEqualTo("value");
    }


    @Test
    public void testIsRetryOrDlqTopicWithRetryTopic() {
        String topic = MixAll.RETRY_GROUP_TOPIC_PREFIX + "TestTopic";
        boolean result = BrokerMetricsManager.isRetryOrDlqTopic(topic);
        assertThat(result).isTrue();
    }

    @Test
    public void testIsRetryOrDlqTopicWithDlqTopic() {
        String topic = MixAll.DLQ_GROUP_TOPIC_PREFIX + "TestTopic";
        boolean result = BrokerMetricsManager.isRetryOrDlqTopic(topic);
        assertThat(result).isTrue();
    }

    @Test
    public void testIsRetryOrDlqTopicWithNonRetryOrDlqTopic() {
        String topic = "NormalTopic";
        boolean result = BrokerMetricsManager.isRetryOrDlqTopic(topic);
        assertThat(result).isFalse();
    }

    @Test
    public void testIsRetryOrDlqTopicWithEmptyTopic() {
        String topic = "";
        boolean result = BrokerMetricsManager.isRetryOrDlqTopic(topic);
        assertThat(result).isFalse();
    }

    @Test
    public void testIsRetryOrDlqTopicWithNullTopic() {
        String topic = null;
        boolean result = BrokerMetricsManager.isRetryOrDlqTopic(topic);
        assertThat(result).isFalse();
    }

    @Test
    public void testIsSystemGroup_SystemGroup_ReturnsTrue() {
        String group = "FooGroup";
        String systemGroup = MixAll.CID_RMQ_SYS_PREFIX + group;
        boolean result = BrokerMetricsManager.isSystemGroup(systemGroup);
        assertThat(result).isTrue();
    }

    @Test
    public void testIsSystemGroup_NonSystemGroup_ReturnsFalse() {
        String group = "FooGroup";
        boolean result = BrokerMetricsManager.isSystemGroup(group);
        assertThat(result).isFalse();
    }

    @Test
    public void testIsSystemGroup_EmptyGroup_ReturnsFalse() {
        String group = "";
        boolean result = BrokerMetricsManager.isSystemGroup(group);
        assertThat(result).isFalse();
    }

    @Test
    public void testIsSystemGroup_NullGroup_ReturnsFalse() {
        String group = null;
        boolean result = BrokerMetricsManager.isSystemGroup(group);
        assertThat(result).isFalse();
    }

    @Test
    public void testIsSystem_SystemTopicOrSystemGroup_ReturnsTrue() {
        String topic = "FooTopic";
        String group = "FooGroup";
        String systemTopic = TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC;
        String systemGroup = MixAll.CID_RMQ_SYS_PREFIX + group;

        boolean resultTopic = BrokerMetricsManager.isSystem(systemTopic, group);
        assertThat(resultTopic).isTrue();

        boolean resultGroup = BrokerMetricsManager.isSystem(topic, systemGroup);
        assertThat(resultGroup).isTrue();
    }

    @Test
    public void testIsSystem_NonSystemTopicAndGroup_ReturnsFalse() {
        String topic = "FooTopic";
        String group = "FooGroup";
        boolean result = BrokerMetricsManager.isSystem(topic, group);
        assertThat(result).isFalse();
    }

    @Test
    public void testIsSystem_EmptyTopicAndGroup_ReturnsFalse() {
        String topic = "";
        String group = "";
        boolean result = BrokerMetricsManager.isSystem(topic, group);
        assertThat(result).isFalse();
    }

    @Test
    public void testGetMessageTypeAsNormal() {
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        requestHeader.setProperties("");

        TopicMessageType result = BrokerMetricsManager.getMessageType(requestHeader);
        assertThat(TopicMessageType.NORMAL).isEqualTo(result);
    }

    @Test
    public void testGetMessageTypeAsTransaction() {
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();

        Map<String, String> map = new HashMap<>();
        map.put(MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");
        requestHeader.setProperties(MessageDecoder.messageProperties2String(map));

        TopicMessageType result = BrokerMetricsManager.getMessageType(requestHeader);
        assertThat(TopicMessageType.TRANSACTION).isEqualTo(result);
    }

    @Test
    public void testGetMessageTypeAsFifo() {
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        Map<String, String> map = new HashMap<>();
        map.put(MessageConst.PROPERTY_SHARDING_KEY, "shardingKey");
        requestHeader.setProperties(MessageDecoder.messageProperties2String(map));

        TopicMessageType result = BrokerMetricsManager.getMessageType(requestHeader);
        assertThat(TopicMessageType.FIFO).isEqualTo(result);
    }

    @Test
    public void testGetMessageTypeAsDelayLevel() {
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        Map<String, String> map = new HashMap<>();
        map.put(MessageConst.PROPERTY_DELAY_TIME_LEVEL, "1");
        requestHeader.setProperties(MessageDecoder.messageProperties2String(map));

        TopicMessageType result = BrokerMetricsManager.getMessageType(requestHeader);
        assertThat(TopicMessageType.DELAY).isEqualTo(result);
    }

    @Test
    public void testGetMessageTypeAsDeliverMS() {
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        Map<String, String> map = new HashMap<>();
        map.put(MessageConst.PROPERTY_TIMER_DELIVER_MS, "10");
        requestHeader.setProperties(MessageDecoder.messageProperties2String(map));

        TopicMessageType result = BrokerMetricsManager.getMessageType(requestHeader);
        assertThat(TopicMessageType.DELAY).isEqualTo(result);
    }

    @Test
    public void testGetMessageTypeAsDelaySEC() {
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        Map<String, String> map = new HashMap<>();
        map.put(MessageConst.PROPERTY_TIMER_DELAY_SEC, "1");
        requestHeader.setProperties(MessageDecoder.messageProperties2String(map));

        TopicMessageType result = BrokerMetricsManager.getMessageType(requestHeader);
        assertThat(TopicMessageType.DELAY).isEqualTo(result);
    }

    @Test
    public void testGetMessageTypeAsDelayMS() {
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        Map<String, String> map = new HashMap<>();
        map.put(MessageConst.PROPERTY_TIMER_DELAY_MS, "10");
        requestHeader.setProperties(MessageDecoder.messageProperties2String(map));

        TopicMessageType result = BrokerMetricsManager.getMessageType(requestHeader);
        assertThat(TopicMessageType.DELAY).isEqualTo(result);
    }

    @Test
    public void testGetMessageTypeWithUnknownProperty() {
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        Map<String, String> map = new HashMap<>();
        map.put("unknownProperty", "unknownValue");
        requestHeader.setProperties(MessageDecoder.messageProperties2String(map));

        TopicMessageType result = BrokerMetricsManager.getMessageType(requestHeader);
        assertThat(TopicMessageType.NORMAL).isEqualTo(result);
    }

    @Test
    public void testGetMessageTypeWithMultipleProperties() {
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        Map<String, String> map = new HashMap<>();
        map.put(MessageConst.PROPERTY_DELAY_TIME_LEVEL, "1");
        map.put(MessageConst.PROPERTY_SHARDING_KEY, "shardingKey");
        requestHeader.setProperties(MessageDecoder.messageProperties2String(map));

        TopicMessageType result = BrokerMetricsManager.getMessageType(requestHeader);
        assertThat(TopicMessageType.FIFO).isEqualTo(result);
    }

    @Test
    public void testGetMessageTypeWithTransactionFlagButOtherPropertiesPresent() {
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        Map<String, String> map = new HashMap<>();
        map.put(MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");
        map.put(MessageConst.PROPERTY_SHARDING_KEY, "shardingKey");
        requestHeader.setProperties(MessageDecoder.messageProperties2String(map));

        TopicMessageType result = BrokerMetricsManager.getMessageType(requestHeader);
        assertThat(TopicMessageType.TRANSACTION).isEqualTo(result);
    }

    @Test
    public void testGetMessageTypeWithEmptyProperties() {
        TopicMessageType result = BrokerMetricsManager.getMessageType(new SendMessageRequestHeader());
        assertThat(TopicMessageType.NORMAL).isEqualTo(result);
    }

    @Test
    public void testCreateMetricsManager() {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        String storePathRootDir = System.getProperty("java.io.tmpdir") + File.separator + "store-"
                + UUID.randomUUID();
        messageStoreConfig.setStorePathRootDir(storePathRootDir);
        BrokerConfig brokerConfig = new BrokerConfig();

        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(0);

        BrokerController brokerController = new BrokerController(brokerConfig, nettyServerConfig,
                new NettyClientConfig(), messageStoreConfig);

        BrokerMetricsManager metricsManager = new BrokerMetricsManager(brokerController);

        assertThat(metricsManager.getBrokerMeter()).isNull();
    }

    @Test
    public void testCreateMetricsManagerLogType() throws CloneNotSupportedException {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setMetricsExporterType(MetricsExporterType.LOG);
        brokerConfig.setMetricsLabel("label1:value1;label2:value2");
        brokerConfig.setMetricsOtelCardinalityLimit(1);

        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        String storePathRootDir = System.getProperty("java.io.tmpdir") + File.separator + "store-"
                + UUID.randomUUID();
        messageStoreConfig.setStorePathRootDir(storePathRootDir);

        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(0);

        BrokerController brokerController = new BrokerController(brokerConfig, nettyServerConfig,
                new NettyClientConfig(), messageStoreConfig);
        brokerController.initialize();

        BrokerMetricsManager metricsManager = new BrokerMetricsManager(brokerController);

        assertThat(metricsManager.getBrokerMeter()).isNotNull();
    }
}