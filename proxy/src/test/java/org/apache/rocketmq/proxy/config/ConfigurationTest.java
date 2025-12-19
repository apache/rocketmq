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

package org.apache.rocketmq.proxy.config;

import org.apache.rocketmq.auth.config.AuthConfig;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.spy;

public class ConfigurationTest {

    private Configuration configuration;

    @Before
    public void init() {
        configuration = spy(new Configuration());
    }

    @Test
    public void testInit() throws Exception {
        configuration.init();

        ProxyConfig loadedProxyConfig = configuration.getProxyConfig();
        assertNotNull(loadedProxyConfig);

        AuthConfig loadedAuthConfig = configuration.getAuthConfig();
        assertNotNull(loadedAuthConfig);
    }
}
