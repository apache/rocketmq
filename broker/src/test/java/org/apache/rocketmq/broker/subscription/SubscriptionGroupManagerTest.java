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
import java.util.Map;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.SubscriptionGroupAttributes;
import org.apache.rocketmq.common.attribute.BooleanAttribute;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SubscriptionGroupManagerTest {
    private String group = "group";
    @Mock
    private BrokerController brokerControllerMock;
    private SubscriptionGroupManager subscriptionGroupManager;

    @Before
    public void before() {
        SubscriptionGroupAttributes.ALL.put("test", new BooleanAttribute(
            "test",
            false,
            false
        ));
        subscriptionGroupManager = spy(new SubscriptionGroupManager(brokerControllerMock));
        when(brokerControllerMock.getMessageStore()).thenReturn(null);
        doNothing().when(subscriptionGroupManager).persist();
    }

    @After
    public void destroy() {
        if (MixAll.isMac()) {
            return;
        }
        if (subscriptionGroupManager != null) {
            subscriptionGroupManager.stop();
        }
    }

    @Test
    public void testUpdateAndCreateSubscriptionGroupInRocksdb() {
        if (MixAll.isMac()) {
            return;
        }
        when(brokerControllerMock.getMessageStoreConfig()).thenReturn(new MessageStoreConfig());
        subscriptionGroupManager = spy(new RocksDBSubscriptionGroupManager(brokerControllerMock));
        subscriptionGroupManager.load();
        group += System.currentTimeMillis();
        updateSubscriptionGroupConfig();
    }

    @Test
    public void updateSubscriptionGroupConfig() {
        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName(group);
        Map<String, String> attr = ImmutableMap.of("+test", "true");
        subscriptionGroupConfig.setAttributes(attr);
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
}