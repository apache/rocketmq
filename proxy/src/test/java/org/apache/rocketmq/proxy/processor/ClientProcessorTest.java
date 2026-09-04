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

package org.apache.rocketmq.proxy.processor;

import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcProxyException;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ClientProcessorTest {

    @Mock
    private MessagingProcessor messagingProcessor;

    @Mock
    private ServiceManager serviceManager;

    @Mock
    private ProxyContext ctx;

    @Mock
    private SubscriptionGroupConfig groupConfig;

    private ClientProcessor clientProcessor;

    @Before
    public void setUp() throws Exception {
        ConfigurationManager.initConfig();
        clientProcessor = new ClientProcessor(messagingProcessor, serviceManager);
    }

    @Test
    public void testValidateLiteMode_regularGroupWithLiteMode_throwsException() {
        String group = "regularGroup";
        when(groupConfig.getLiteBindTopic()).thenReturn("");
        when(messagingProcessor.getSubscriptionGroupConfig(ctx, group)).thenReturn(groupConfig);

        GrpcProxyException exception = assertThrows(GrpcProxyException.class, () -> {
            clientProcessor.validateLiteMode(ctx, group, MessageModel.LITE_SELECTIVE);
        });

        assertEquals("regular group cannot use LITE mode: " + group, exception.getMessage());
    }

    @Test
    public void testValidateLiteMode_liteGroupWithoutLiteMode_throwsException() {
        String group = "liteGroup";
        when(groupConfig.getLiteBindTopic()).thenReturn("topic1");
        when(messagingProcessor.getSubscriptionGroupConfig(ctx, group)).thenReturn(groupConfig);

        GrpcProxyException exception = assertThrows(GrpcProxyException.class, () -> {
            clientProcessor.validateLiteMode(ctx, group, MessageModel.CLUSTERING);
        });

        assertEquals("lite group must use LITE mode: " + group, exception.getMessage());
    }

    @Test
    public void testValidateLiteMode_regularGroupWithoutLiteMode_noException() {
        String group = "regularGroup";
        when(groupConfig.getLiteBindTopic()).thenReturn("");
        when(messagingProcessor.getSubscriptionGroupConfig(ctx, group)).thenReturn(groupConfig);

        assertDoesNotThrow(() -> {
            clientProcessor.validateLiteMode(ctx, group, MessageModel.CLUSTERING);
        });
    }

    @Test
    public void testValidateLiteMode_liteGroupWithLiteMode_noException() {
        String group = "liteGroup";
        when(groupConfig.getLiteBindTopic()).thenReturn("topic1");
        when(messagingProcessor.getSubscriptionGroupConfig(ctx, group)).thenReturn(groupConfig);

        assertDoesNotThrow(() -> {
            clientProcessor.validateLiteMode(ctx, group, MessageModel.LITE_SELECTIVE);
        });
    }

    @Test
    public void testValidateLiteSubTopic_emptySubList_noException() {
        String group = "group";
        Set<SubscriptionData> subList = new HashSet<>();

        assertDoesNotThrow(() -> {
            clientProcessor.validateLiteSubTopic(ctx, group, subList);
        });
    }

    @Test
    public void testValidateLiteSubTopic_validSubList_noException() {
        String group = "group";
        String topic = "topic1";
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(topic);
        Set<SubscriptionData> subList = new HashSet<>();
        subList.add(subscriptionData);

        when(groupConfig.getLiteBindTopic()).thenReturn(topic);
        when(messagingProcessor.getSubscriptionGroupConfig(ctx, group)).thenReturn(groupConfig);

        assertDoesNotThrow(() -> {
            clientProcessor.validateLiteSubTopic(ctx, group, subList);
        });
    }

    @Test
    public void testValidateLiteBindTopic_matchingTopics_noException() {
        String group = "group";
        String bindTopic = "topic1";

        when(groupConfig.getLiteBindTopic()).thenReturn(bindTopic);
        when(messagingProcessor.getSubscriptionGroupConfig(ctx, group)).thenReturn(groupConfig);

        assertDoesNotThrow(() -> {
            clientProcessor.validateLiteBindTopic(ctx, group, bindTopic);
        });
    }

    @Test
    public void testValidateLiteBindTopic_mismatchedTopics_throwsException() {
        String group = "group";
        String expectedTopic = "expectedTopic";
        String actualTopic = "actualTopic";

        when(groupConfig.getLiteBindTopic()).thenReturn(expectedTopic);
        when(messagingProcessor.getSubscriptionGroupConfig(ctx, group)).thenReturn(groupConfig);

        GrpcProxyException exception = assertThrows(GrpcProxyException.class, () -> {
            clientProcessor.validateLiteBindTopic(ctx, group, actualTopic);
        });

        assertTrue(exception.getMessage().contains("expected to bind topic"));
    }

    @Test
    public void testValidateLiteSubscriptionQuota_withinQuota_noException() {
        String group = "group";
        int quota = 10;
        int actual = 5;

        when(groupConfig.getLiteSubClientQuota()).thenReturn(quota);
        when(messagingProcessor.getSubscriptionGroupConfig(ctx, group)).thenReturn(groupConfig);

        assertDoesNotThrow(() -> {
            clientProcessor.validateLiteSubscriptionQuota(ctx, group, actual);
        });
    }

    @Test
    public void testValidateLiteSubscriptionQuota_exceedsQuota_throwsException() {
        String group = "group";
        int quota = 10;
        int actual = 15 + 300 /*quota buffer*/;

        when(groupConfig.getLiteSubClientQuota()).thenReturn(quota);
        when(messagingProcessor.getSubscriptionGroupConfig(ctx, group)).thenReturn(groupConfig);

        GrpcProxyException exception = assertThrows(GrpcProxyException.class, () -> {
            clientProcessor.validateLiteSubscriptionQuota(ctx, group, actual);
        });

        assertTrue(exception.getMessage().contains("lite subscription quota exceeded"));
    }

    @Test
    public void testGetGroupOrException_groupExists_returnsConfig() {
        String group = "group";
        when(messagingProcessor.getSubscriptionGroupConfig(ctx, group)).thenReturn(groupConfig);

        SubscriptionGroupConfig result = clientProcessor.getGroupOrException(ctx, group);
        assertEquals(groupConfig, result);
    }

    @Test
    public void testGetGroupOrException_groupNotExists_throwsException() {
        String group = "nonExistentGroup";
        when(messagingProcessor.getSubscriptionGroupConfig(ctx, group)).thenReturn(null);

        GrpcProxyException exception = assertThrows(GrpcProxyException.class, () -> {
            clientProcessor.getGroupOrException(ctx, group);
        });

        assertEquals("group not found: " + group, exception.getMessage());
    }
}
