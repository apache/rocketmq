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

package org.apache.rocketmq.broker.lite;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LiteMetadataUtilTest {

    @Mock
    private BrokerController brokerController;

    @Mock
    private SubscriptionGroupManager subscriptionGroupManager;

    @Before
    public void setUp() {
        when(brokerController.getSubscriptionGroupManager()).thenReturn(subscriptionGroupManager);
    }

    @Test
    public void testIsLiteGroupTypeTreatsEmptyBindTopicAsNonLite() {
        SubscriptionGroupConfig emptyBindGroup = new SubscriptionGroupConfig();
        emptyBindGroup.setGroupName("emptyBindGroup");
        emptyBindGroup.setLiteBindTopic("");

        SubscriptionGroupConfig blankBindGroup = new SubscriptionGroupConfig();
        blankBindGroup.setGroupName("blankBindGroup");
        blankBindGroup.setLiteBindTopic(" ");

        SubscriptionGroupConfig liteGroup = new SubscriptionGroupConfig();
        liteGroup.setGroupName("liteGroup");
        liteGroup.setLiteBindTopic("parentTopic");

        when(subscriptionGroupManager.findSubscriptionGroupConfig("normalGroup"))
            .thenReturn(new SubscriptionGroupConfig());
        when(subscriptionGroupManager.findSubscriptionGroupConfig("emptyBindGroup"))
            .thenReturn(emptyBindGroup);
        when(subscriptionGroupManager.findSubscriptionGroupConfig("blankBindGroup"))
            .thenReturn(blankBindGroup);
        when(subscriptionGroupManager.findSubscriptionGroupConfig("liteGroup"))
            .thenReturn(liteGroup);

        assertFalse(LiteMetadataUtil.isLiteGroupType("missingGroup", brokerController));
        assertFalse(LiteMetadataUtil.isLiteGroupType("normalGroup", brokerController));
        assertFalse(LiteMetadataUtil.isLiteGroupType("emptyBindGroup", brokerController));
        assertFalse(LiteMetadataUtil.isLiteGroupType("blankBindGroup", brokerController));
        assertTrue(LiteMetadataUtil.isLiteGroupType("liteGroup", brokerController));
    }

    @Test
    public void testGetSubscriberGroupMapSkipsEmptyBindTopic() {
        ConcurrentMap<String, SubscriptionGroupConfig> groupTable = new ConcurrentHashMap<>();
        groupTable.put("normalGroup", new SubscriptionGroupConfig());

        SubscriptionGroupConfig emptyBindGroup = new SubscriptionGroupConfig();
        emptyBindGroup.setGroupName("emptyBindGroup");
        emptyBindGroup.setLiteBindTopic("");
        groupTable.put("emptyBindGroup", emptyBindGroup);

        SubscriptionGroupConfig blankBindGroup = new SubscriptionGroupConfig();
        blankBindGroup.setGroupName("blankBindGroup");
        blankBindGroup.setLiteBindTopic(" ");
        groupTable.put("blankBindGroup", blankBindGroup);

        SubscriptionGroupConfig liteGroup = new SubscriptionGroupConfig();
        liteGroup.setGroupName("liteGroup");
        liteGroup.setLiteBindTopic("parentTopic");
        groupTable.put("liteGroup", liteGroup);

        when(subscriptionGroupManager.getSubscriptionGroupTable()).thenReturn(groupTable);

        Map<String, Set<String>> result = LiteMetadataUtil.getSubscriberGroupMap(brokerController);

        assertFalse(result.containsKey(""));
        assertFalse(result.containsKey(" "));
        assertFalse(result.containsKey(null));
        assertEquals(Collections.singleton("liteGroup"), result.get("parentTopic"));
    }
}
