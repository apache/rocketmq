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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class BrokerContainerProcessorConfigSanitizeTest {

    @Test
    public void testSanitizeRemovesSensitiveKeys() {
        String content = "brokerName=broker-a\n"
            + "initAuthenticationUser={\"username\":\"rocketmq\",\"password\":\"secret\"}\n"
            + "innerClientAuthenticationCredentials={\"accessKey\":\"ak\",\"secretKey\":\"sk\"}\n"
            + "listenPort=10911\n";

        String sanitized = BrokerContainerProcessor.sanitizeConfigForResponse(content);

        assertFalse(sanitized.contains("initAuthenticationUser"));
        assertFalse(sanitized.contains("innerClientAuthenticationCredentials"));
        assertFalse(sanitized.contains("secret"));
        assertTrue(sanitized.contains("brokerName=broker-a"));
        assertTrue(sanitized.contains("listenPort=10911"));
    }

    @Test
    public void testSanitizeFailsClosedOnUnparsableContent() {
        // malformed unicode escape makes Properties.load throw
        assertNull(BrokerContainerProcessor.sanitizeConfigForResponse("key=\\uZZZZ\n"));
    }

    @Test
    public void testSanitizePassesThroughNullOrEmpty() {
        assertNull(BrokerContainerProcessor.sanitizeConfigForResponse(null));
        assertEquals("", BrokerContainerProcessor.sanitizeConfigForResponse(""));
    }
}
