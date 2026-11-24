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

package org.apache.rocketmq.proxy.grpc.v2.common;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class GrpcValidatorTest {

    private GrpcValidator grpcValidator;

    @Before
    public void before() {
        this.grpcValidator = GrpcValidator.getInstance();
    }

    @Test
    public void testValidateTopic() {
        assertThrows(GrpcProxyException.class, () -> grpcValidator.validateTopic(""));
        assertThrows(GrpcProxyException.class, () -> grpcValidator.validateTopic("rmq_sys_xxxx"));
        grpcValidator.validateTopic("topicName");
    }

    @Test
    public void testValidateConsumerGroup() {
        assertThrows(GrpcProxyException.class, () -> grpcValidator.validateConsumerGroup(""));
        assertThrows(GrpcProxyException.class, () -> grpcValidator.validateConsumerGroup("CID_RMQ_SYS_xxxx"));
        grpcValidator.validateConsumerGroup("consumerGroupName");
    }


    @Test
    public void testValidateLiteTopic_Null() {
        assertThrows(GrpcProxyException.class, () -> grpcValidator.validateLiteTopic(null));
    }

    @Test
    public void testValidateLiteTopic_Blank() {
        assertThrows(GrpcProxyException.class, () -> grpcValidator.validateLiteTopic("   "));
    }

    @Test
    public void testValidateLiteTopic_TooLong() {
        try (MockedStatic<ConfigurationManager> mockedConfig = mockStatic(ConfigurationManager.class)) {
            ProxyConfig proxyConfig = mock(ProxyConfig.class);
            when(proxyConfig.getMaxLiteTopicSize()).thenReturn(5);
            mockedConfig.when(ConfigurationManager::getProxyConfig).thenReturn(proxyConfig);

            assertThrows(GrpcProxyException.class, () -> grpcValidator.validateLiteTopic("toolongtopic"));
        }
    }

    @Test
    public void testValidateLiteTopic_IllegalCharacter() {
        try (MockedStatic<ConfigurationManager> mockedConfig = mockStatic(ConfigurationManager.class)) {
            ProxyConfig proxyConfig = mock(ProxyConfig.class);
            when(proxyConfig.getMaxLiteTopicSize()).thenReturn(100);
            mockedConfig.when(ConfigurationManager::getProxyConfig).thenReturn(proxyConfig);

            assertThrows(GrpcProxyException.class, () -> grpcValidator.validateLiteTopic("invalid@topic"));

            assertThrows(GrpcProxyException.class, () -> grpcValidator.validateLiteTopic("invalid$topic"));

            assertThrows(GrpcProxyException.class, () -> grpcValidator.validateLiteTopic("invalid%topic"));

            assertThrows(GrpcProxyException.class, () -> grpcValidator.validateLiteTopic("invalid\ttopic"));

            assertThrows(GrpcProxyException.class, () -> grpcValidator.validateLiteTopic("invalid\ntopic"));

            assertThrows(GrpcProxyException.class, () -> grpcValidator.validateLiteTopic("invalid\0topic"));
        }
    }

    @Test
    public void testValidateLiteTopic_Valid() {
        try (MockedStatic<ConfigurationManager> mockedConfig = mockStatic(ConfigurationManager.class)) {
            ProxyConfig proxyConfig = mock(ProxyConfig.class);
            when(proxyConfig.getMaxLiteTopicSize()).thenReturn(64);
            mockedConfig.when(ConfigurationManager::getProxyConfig).thenReturn(proxyConfig);

            grpcValidator.validateLiteTopic("Valid_Topic-123");

            grpcValidator.validateLiteTopic(RandomStringUtils.randomAlphanumeric(64));

            grpcValidator.validateLiteTopic(RandomStringUtils.randomAlphanumeric(63));
        }
    }
}
