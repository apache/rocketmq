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
package org.apache.rocketmq.container;

import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class BrokerContainerProcessorConfigSanitizeTest {

    @Test
    public void testSanitizeRemovesSensitiveKeys() {
        Properties properties = new Properties();
        properties.setProperty("brokerName", "broker-a");
        properties.setProperty("initAuthenticationUser", "sensitive-user-config");
        properties.setProperty("innerClientAuthenticationCredentials", "sensitive-client-config");
        properties.setProperty("listenPort", "10911");

        String sanitized = BrokerContainerProcessor.sanitizeConfigForResponse(properties);

        assertFalse(sanitized.contains("initAuthenticationUser"));
        assertFalse(sanitized.contains("innerClientAuthenticationCredentials"));
        assertFalse(sanitized.contains("sensitive"));
        assertTrue(sanitized.contains("brokerName=broker-a"));
        assertTrue(sanitized.contains("listenPort=10911"));
        assertEquals(4, properties.size());
    }

    @Test
    public void testSanitizePreservesConfigValues() {
        Properties properties = new Properties();
        properties.setProperty("customPath", "C:\\rocketmq\\store");
        properties.setProperty("label", "\u4e2d\u6587");
        properties.setProperty("regex", "^foo\\d+$");

        String sanitized = BrokerContainerProcessor.sanitizeConfigForResponse(properties);

        assertTrue(sanitized.contains("customPath=C:\\rocketmq\\store"));
        assertTrue(sanitized.contains("label=\u4e2d\u6587"));
        assertTrue(sanitized.contains("regex=^foo\\d+$"));
    }

    @Test
    public void testSanitizeHandlesNullOrEmpty() {
        assertNull(BrokerContainerProcessor.sanitizeConfigForResponse(null));
        assertEquals("", BrokerContainerProcessor.sanitizeConfigForResponse(new Properties()));
    }
}
