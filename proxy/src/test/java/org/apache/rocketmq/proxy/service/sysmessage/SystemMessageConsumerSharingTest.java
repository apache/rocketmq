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
package org.apache.rocketmq.proxy.service.sysmessage;

import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.service.admin.AdminService;
import org.apache.rocketmq.proxy.service.client.ClusterConsumerManager;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

public class SystemMessageConsumerSharingTest extends InitConfigTest {
    @Test
    public void testManagerAndAdditionalSyncerReceiveSameExecutor() throws Exception {
        ThreadPoolExecutor executor = SystemMessageConsumeExecutor.create(ConfigurationManager.getProxyConfig());
        TopicRouteService routeService = mock(TopicRouteService.class);
        AdminService adminService = mock(AdminService.class);
        MQClientAPIFactory clientFactory = mock(MQClientAPIFactory.class);
        ClusterConsumerManager manager = new ClusterConsumerManager(routeService, adminService, clientFactory,
            mock(ConsumerIdsChangeListener.class), 120000, null, executor);
        HeartbeatSyncer heartbeat = (HeartbeatSyncer) FieldUtils.readDeclaredField(manager, "heartbeatSyncer", true);
        AbstractSystemMessageSyncer additional = new AbstractSystemMessageSyncer(routeService, adminService,
            clientFactory, null, executor) {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages, ConsumeConcurrentlyContext context) {
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        };
        try {
            assertSame(executor, heartbeat.consumeExecutor);
            assertSame(executor, additional.consumeExecutor);
            assertFalse(executor.isShutdown());
        } finally {
            heartbeat.threadPoolExecutor.shutdownNow();
            executor.shutdownNow();
        }
    }
}
