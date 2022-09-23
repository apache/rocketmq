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

package org.apache.rocketmq.proxy;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerStartup;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.proxy.config.Configuration;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.processor.DefaultMessagingProcessor;
import org.assertj.core.util.Strings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import static org.apache.rocketmq.proxy.config.ConfigurationManager.RMQ_PROXY_HOME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

public class ProxyStartupTest {

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
    }

    @After
    public void after() {
        System.clearProperty(RMQ_PROXY_HOME);
        System.clearProperty(MixAll.NAMESRV_ADDR_PROPERTY);
        System.clearProperty(Configuration.CONFIG_PATH_PROPERTY);
        System.clearProperty(ClientLogger.CLIENT_LOG_USESLF4J);
    }

    @Test
    public void testParseAndInitCommandLineArgument() throws Exception {
        Path configFilePath = Files.createTempFile("testParseAndInitCommandLineArgument", ".json");
        String configData = "{}";
        Files.write(configFilePath, configData.getBytes(StandardCharsets.UTF_8));

        String brokerConfigPath = "brokerConfigPath";
        String proxyConfigPath = configFilePath.toAbsolutePath().toString();
        String proxyMode = "LOCAL";
        String namesrvAddr = "namesrvAddr";
        CommandLineArgument commandLineArgument = ProxyStartup.parseCommandLineArgument(new String[] {
            "-bc", brokerConfigPath,
            "-pc", proxyConfigPath,
            "-pm", proxyMode,
            "-n", namesrvAddr
        });

        assertEquals(brokerConfigPath, commandLineArgument.getBrokerConfigPath());
        assertEquals(proxyConfigPath, commandLineArgument.getProxyConfigPath());
        assertEquals(proxyMode, commandLineArgument.getProxyMode());
        assertEquals(namesrvAddr, commandLineArgument.getNamesrvAddr());

        ProxyStartup.initLogAndConfiguration(commandLineArgument);

        ProxyConfig config = ConfigurationManager.getProxyConfig();
        assertEquals(brokerConfigPath, config.getBrokerConfigPath());
        assertEquals(proxyMode, config.getProxyMode());
        assertEquals(namesrvAddr, config.getNamesrvAddr());
    }

    @Test
    public void testLocalModeWithNameSrvAddrByProperty() throws Exception {
        String namesrvAddr = "namesrvAddr";
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, namesrvAddr);
        CommandLineArgument commandLineArgument = ProxyStartup.parseCommandLineArgument(new String[] {
            "-pm", "local"
        });
        ProxyStartup.initLogAndConfiguration(commandLineArgument);

        ProxyConfig config = ConfigurationManager.getProxyConfig();
        assertEquals(namesrvAddr, config.getNamesrvAddr());

        validateBrokerCreateArgsWithNamsrvAddr(config, namesrvAddr);
    }

    private void validateBrokerCreateArgsWithNamsrvAddr(ProxyConfig config, String namesrvAddr) {
        try (MockedStatic<BrokerStartup> brokerStartupMocked = mockStatic(BrokerStartup.class);
             MockedStatic<DefaultMessagingProcessor> messagingProcessorMocked = mockStatic(DefaultMessagingProcessor.class)) {
            ArgumentCaptor<Object> args = ArgumentCaptor.forClass(Object.class);
            brokerStartupMocked.when(() -> BrokerStartup.createBrokerController((String[]) args.capture()))
                .thenReturn(mock(BrokerController.class));
            messagingProcessorMocked.when(() -> DefaultMessagingProcessor.createForLocalMode(any(), any()))
                .thenReturn(mock(DefaultMessagingProcessor.class));

            ProxyStartup.createMessagingProcessor();
            String[] passArgs = (String[]) args.getValue();
            assertEquals("-c", passArgs[0]);
            assertEquals(config.getBrokerConfigPath(), passArgs[1]);
            assertEquals("-n", passArgs[2]);
            assertEquals(namesrvAddr, passArgs[3]);
            assertEquals(4, passArgs.length);
        }
    }

    @Test
    public void testLocalModeWithNameSrvAddrByConfigFile() throws Exception {
        String namesrvAddr = "namesrvAddr";
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "foo");
        Path configFilePath = Files.createTempFile("testLocalModeWithNameSrvAddrByConfigFile", ".json");
        String configData = "{\n" +
            "  \"namesrvAddr\": \"namesrvAddr\"\n" +
            "}";
        Files.write(configFilePath, configData.getBytes(StandardCharsets.UTF_8));

        CommandLineArgument commandLineArgument = ProxyStartup.parseCommandLineArgument(new String[] {
            "-pm", "local",
            "-pc", configFilePath.toAbsolutePath().toString()
        });
        ProxyStartup.initLogAndConfiguration(commandLineArgument);

        ProxyConfig config = ConfigurationManager.getProxyConfig();
        assertEquals(namesrvAddr, config.getNamesrvAddr());

        validateBrokerCreateArgsWithNamsrvAddr(config, namesrvAddr);
    }

    @Test
    public void testLocalModeWithNameSrvAddrByCommandLine() throws Exception {
        String namesrvAddr = "namesrvAddr";
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "foo");
        Path configFilePath = Files.createTempFile("testLocalModeWithNameSrvAddrByCommandLine", ".json");
        String configData = "{\n" +
            "  \"namesrvAddr\": \"foo\"\n" +
            "}";
        Files.write(configFilePath, configData.getBytes(StandardCharsets.UTF_8));

        CommandLineArgument commandLineArgument = ProxyStartup.parseCommandLineArgument(new String[] {
            "-pm", "local",
            "-pc", configFilePath.toAbsolutePath().toString(),
            "-n", namesrvAddr
        });
        ProxyStartup.initLogAndConfiguration(commandLineArgument);

        ProxyConfig config = ConfigurationManager.getProxyConfig();
        assertEquals(namesrvAddr, config.getNamesrvAddr());

        validateBrokerCreateArgsWithNamsrvAddr(config, namesrvAddr);
    }

    @Test
    public void testLocalModeWithAllArgs() throws Exception {
        String namesrvAddr = "namesrvAddr";
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "foo");
        Path configFilePath = Files.createTempFile("testLocalMode", ".json");
        String configData = "{\n" +
            "  \"namesrvAddr\": \"foo\"\n" +
            "}";
        Files.write(configFilePath, configData.getBytes(StandardCharsets.UTF_8));
        Path brokerConfigFilePath = Files.createTempFile("testLocalModeBrokerConfig", ".json");

        CommandLineArgument commandLineArgument = ProxyStartup.parseCommandLineArgument(new String[] {
            "-pm", "local",
            "-pc", configFilePath.toAbsolutePath().toString(),
            "-n", namesrvAddr,
            "-bc", brokerConfigFilePath.toAbsolutePath().toString()
        });
        ProxyStartup.initLogAndConfiguration(commandLineArgument);

        ProxyConfig config = ConfigurationManager.getProxyConfig();
        assertEquals(namesrvAddr, config.getNamesrvAddr());
        assertEquals(brokerConfigFilePath.toAbsolutePath().toString(), config.getBrokerConfigPath());

        validateBrokerCreateArgsWithNamsrvAddr(config, namesrvAddr);
    }

    @Test
    public void testClusterMode() throws Exception {
        CommandLineArgument commandLineArgument = ProxyStartup.parseCommandLineArgument(new String[] {
            "-pm", "cluster"
        });
        ProxyStartup.initLogAndConfiguration(commandLineArgument);

        try (MockedStatic<DefaultMessagingProcessor> messagingProcessorMocked = mockStatic(DefaultMessagingProcessor.class)) {
            DefaultMessagingProcessor processor = mock(DefaultMessagingProcessor.class);
            messagingProcessorMocked.when(DefaultMessagingProcessor::createForClusterMode)
                .thenReturn(processor);

            assertSame(processor, ProxyStartup.createMessagingProcessor());
        }
    }
}