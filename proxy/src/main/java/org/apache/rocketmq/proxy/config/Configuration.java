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

import com.alibaba.fastjson.JSON;
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Configuration {
    private final static Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private final AtomicReference<ProxyConfig> proxyConfigReference = new AtomicReference<>();
    public static final String CONFIG_PATH_PROPERTY = "com.rocketmq.proxy.configPath";

    public void init() throws Exception {
        String proxyConfigData = loadJsonConfig();

        ProxyConfig proxyConfig = JSON.parseObject(proxyConfigData, ProxyConfig.class);
        proxyConfig.initData();
        setProxyConfig(proxyConfig);
    }

    public static String loadJsonConfig() throws Exception {
        String configFileName = ProxyConfig.DEFAULT_CONFIG_FILE_NAME;
        String filePath = System.getProperty(CONFIG_PATH_PROPERTY);
        if (StringUtils.isBlank(filePath)) {
            final String testResource = "rmq-proxy-home/conf/" + configFileName;
            try (InputStream inputStream = Configuration.class.getClassLoader().getResourceAsStream(testResource)) {
                if (null != inputStream) {
                    return CharStreams.toString(new InputStreamReader(inputStream, Charsets.UTF_8));
                }
            }
            filePath = new File(ConfigurationManager.getProxyHome() + File.separator + "conf", configFileName).toString();
        }

        File file = new File(filePath);
        if (!file.exists()) {
            log.warn("the config file {} not exist", filePath);
            throw new RuntimeException(String.format("the config file %s not exist", filePath));
        }
        long fileLength = file.length();
        if (fileLength <= 0) {
            log.warn("the config file {} length is zero", filePath);
            throw new RuntimeException(String.format("the config file %s length is zero", filePath));
        }

        return new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
    }

    public ProxyConfig getProxyConfig() {
        return proxyConfigReference.get();
    }

    public void setProxyConfig(ProxyConfig proxyConfig) {
        proxyConfigReference.set(proxyConfig);
    }
}
