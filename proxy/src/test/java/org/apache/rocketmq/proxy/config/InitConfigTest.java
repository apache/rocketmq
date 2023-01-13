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

import java.net.URL;
import org.assertj.core.util.Strings;

import org.junit.After;
import org.junit.Before;

import static org.apache.rocketmq.proxy.config.ConfigurationManager.RMQ_PROXY_HOME;

public class InitConfigTest {
    public static String mockProxyHome = "/mock/rmq/proxy/home";

    @Before
    public void before() throws Throwable {
        URL mockProxyHomeURL = getClass().getClassLoader().getResource("rmq-proxy-home");
        if (mockProxyHomeURL != null) {
            mockProxyHome = mockProxyHomeURL.toURI().getPath();
        }

        if (!Strings.isNullOrEmpty(mockProxyHome)) {
            System.setProperty(RMQ_PROXY_HOME, mockProxyHome);
        }

        ConfigurationManager.initEnv();
        ConfigurationManager.intConfig();
    }

    @After
    public void after() {
        System.clearProperty(RMQ_PROXY_HOME);
    }
}