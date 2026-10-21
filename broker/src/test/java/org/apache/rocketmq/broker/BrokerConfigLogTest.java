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

package org.apache.rocketmq.broker;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.io.File;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.broker.processor.AdminBrokerProcessor;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.logging.ch.qos.logback.classic.Level;
import org.apache.rocketmq.logging.ch.qos.logback.classic.Logger;
import org.apache.rocketmq.logging.ch.qos.logback.classic.spi.ILoggingEvent;
import org.apache.rocketmq.logging.ch.qos.logback.core.read.ListAppender;
import org.apache.rocketmq.remoting.Configuration;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BrokerConfigLogTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testAuthConfigUpdatesMaskLogsAndPreserveRawValues() throws Exception {
        BrokerConfig brokerConfig = new BrokerConfig();
        File configFile = temporaryFolder.newFile("broker.properties");
        brokerConfig.setBrokerConfigPath(configFile.getAbsolutePath());
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setStorePathRootDir(temporaryFolder.newFolder("store").getAbsolutePath());
        AuthConfig authConfig = new AuthConfig();
        BrokerController controller = new BrokerController(brokerConfig, new NettyServerConfig(),
            new NettyClientConfig(), storeConfig, authConfig);
        Logger logger = (Logger) BrokerController.LOG;
        Level originalLevel = logger.getLevel();
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(Level.INFO);
        try {
            Configuration configuration = controller.getConfiguration();
            assertThat(configuration.getAllConfigs()).containsEntry("authenticationEnabled", "false");
            // Populate bootstrap data after construction to avoid initializing authentication storage.
            String oldUser = "{\"username\":\"admin\",\"password\":\"old-password\"}";
            String newUser = "{\"username\":\"admin\",\"password\":\"new-password\"}";
            String oldCredentials = "{\"accessKey\":\"test-ak\",\"secretKey\":\"old-secret\"}";
            String newCredentials = "{\"accessKey\":\"test-ak\",\"secretKey\":\"new-secret\"}";
            authConfig.setInitAuthenticationUser(oldUser);
            authConfig.setInnerClientAuthenticationCredentials(oldCredentials);
            Properties initial = new Properties();
            initial.setProperty("initAuthenticationUser", oldUser);
            initial.setProperty("innerClientAuthenticationCredentials", oldCredentials);
            // BrokerStartup registers the original file properties after constructing the controller.
            configuration.registerConfig(initial);
            Properties update = new Properties();
            update.setProperty("initAuthenticationUser", newUser);
            update.setProperty("innerClientAuthenticationCredentials", newCredentials);
            update.setProperty("authenticationEnabled", "true");
            update.setProperty("authenticationWhitelist", "11, 12");
            ChannelHandlerContext context = mock(ChannelHandlerContext.class);
            Channel channel = mock(Channel.class);
            when(context.channel()).thenReturn(channel);
            when(channel.remoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 12345));
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_BROKER_CONFIG, null);
            request.setBody(MixAll.properties2String(update).getBytes(StandardCharsets.UTF_8));
            RemotingCommand response = new AdminBrokerProcessor(controller).processRequest(context, request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

            String logs = appender.list.stream().map(ILoggingEvent::getFormattedMessage)
                .collect(Collectors.joining("\n"));
            assertThat(appender.list.stream().map(ILoggingEvent::getFormattedMessage)
                .filter(message -> message.startsWith("updateBrokerConfig, new config:")).collect(Collectors.toList()))
                .singleElement().asString()
                .contains("initAuthenticationUser=******", "innerClientAuthenticationCredentials=******",
                    "authenticationEnabled=true", "authenticationWhitelist=11, 12", "client: 127.0.0.1:12345");
            assertThat(logs).contains("Replace, key: initAuthenticationUser, value: ****** -> ******");
            assertThat(logs).contains("Replace, key: innerClientAuthenticationCredentials, value: ****** -> ******");
            assertThat(logs).doesNotContain(oldUser, newUser, oldCredentials, newCredentials,
                "old-password", "new-password", "old-secret", "new-secret");
            assertThat(authConfig.getInitAuthenticationUser()).isEqualTo(newUser);
            assertThat(authConfig.getInnerClientAuthenticationCredentials()).isEqualTo(newCredentials);
            assertThat(authConfig.isAuthenticationEnabled()).isTrue();
            assertThat(authConfig.isAuthenticationRequired("11")).isFalse();
            assertThat(authConfig.isAuthenticationRequired("13")).isTrue();
            assertThat(configuration.getAllConfigsSnapshot()).containsAllEntriesOf(update);
            Properties persisted = MixAll.string2Properties(MixAll.file2String(configFile));
            assertThat(persisted).containsAllEntriesOf(update);
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(originalLevel);
            appender.stop();
            controller.shutdown();
        }
    }
}
