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

package org.apache.rocketmq.proxy.common;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ProxyContextTest {
    private ProxyContext proxyContext;

    @Test
    public void testWithValue() {
        String key = "key";
        String value = "value";
        proxyContext = ProxyContext.create();
        ProxyContext newContext = proxyContext.withValue(key, value);
        assertThat(newContext.getValue(key, String.class)).isEqualTo(value);
        String actualValue = newContext.getValue(key);
        assertThat(actualValue).isEqualTo(value);

        assertThat(proxyContext.getValue(key, String.class)).isNull();
    }

    @Test
    public void testSetLocalAddress() {
        String address = "address";
        proxyContext = ProxyContext.create();
        ProxyContext newProxyContext = proxyContext.withLocalAddress(address);
        assertThat(proxyContext.getLocalAddress()).isNull();
        assertThat(newProxyContext.getLocalAddress()).isEqualTo(address);
    }
}