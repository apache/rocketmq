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

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter.Feature;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.MixAll;

public class ConfigurationManager {
    public static final String RMQ_PROXY_HOME = "RMQ_PROXY_HOME";
    protected static final String DEFAULT_RMQ_PROXY_HOME = MixAll.ROCKETMQ_HOME_DIR;
    protected static String proxyHome;
    protected static Configuration configuration;

    public static void initEnv() {
        proxyHome = System.getenv(RMQ_PROXY_HOME);
        if (StringUtils.isEmpty(proxyHome)) {
            proxyHome = System.getProperty(RMQ_PROXY_HOME, DEFAULT_RMQ_PROXY_HOME);
        }

        if (proxyHome == null) {
            proxyHome = "./";
        }
    }

    public static void intConfig() throws Exception {
        configuration = new Configuration();
        configuration.init();
    }

    public static String getProxyHome() {
        return proxyHome;
    }

    public static ProxyConfig getProxyConfig() {
        return configuration.getProxyConfig();
    }

    public static AuthConfig getAuthConfig() {
        return configuration.getAuthConfig();
    }

    public static String formatProxyConfig() {
        return JSON.toJSONString(ConfigurationManager.getProxyConfig(),
                Feature.PrettyFormat, Feature.WriteMapNullValue, Feature.WriteNullListAsEmpty);
    }
}
