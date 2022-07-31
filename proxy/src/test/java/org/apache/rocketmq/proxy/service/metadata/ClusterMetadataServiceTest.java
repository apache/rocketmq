/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.service.metadata;

import java.util.HashMap;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.statictopic.TopicConfigAndQueueMapping;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.service.BaseServiceTest;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

public class ClusterMetadataServiceTest extends BaseServiceTest {

    private ClusterMetadataService clusterMetadataService;

    @Before
    public void before() throws Throwable {
        super.before();
        ConfigurationManager.getProxyConfig().setRocketMQClusterName(CLUSTER_NAME);

        TopicConfigAndQueueMapping topicConfigAndQueueMapping = new TopicConfigAndQueueMapping();
        topicConfigAndQueueMapping.setAttributes(new HashMap<>());
        topicConfigAndQueueMapping.setTopicMessageType(TopicMessageType.NORMAL);
        when(this.mqClientAPIExt.getTopicConfig(anyString(), eq(TOPIC), anyLong())).thenReturn(topicConfigAndQueueMapping);

        when(this.mqClientAPIExt.getSubscriptionGroupConfig(anyString(), eq(GROUP), anyLong())).thenReturn(new SubscriptionGroupConfig());

        this.clusterMetadataService = new ClusterMetadataService(this.topicRouteService, this.mqClientAPIFactory);
    }

    @Test
    public void testGetTopicMessageType() {
        assertEquals(TopicMessageType.UNSPECIFIED, this.clusterMetadataService.getTopicMessageType(ERR_TOPIC));
        assertEquals(1, this.clusterMetadataService.topicConfigCache.asMap().size());
        assertEquals(TopicMessageType.UNSPECIFIED, this.clusterMetadataService.getTopicMessageType(ERR_TOPIC));

        assertEquals(TopicMessageType.NORMAL, this.clusterMetadataService.getTopicMessageType(TOPIC));
        assertEquals(2, this.clusterMetadataService.topicConfigCache.asMap().size());
    }

    @Test
    public void testGetSubscriptionGroupConfig() {
        assertNotNull(this.clusterMetadataService.getSubscriptionGroupConfig(GROUP));
        assertEquals(1, this.clusterMetadataService.subscriptionGroupConfigCache.asMap().size());
    }
}