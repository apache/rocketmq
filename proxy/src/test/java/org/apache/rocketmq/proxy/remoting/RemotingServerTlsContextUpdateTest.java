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
package org.apache.rocketmq.proxy.remoting;

import io.netty.handler.ssl.SslContext;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.cert.TlsCertificateManager;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;

@ExtendWith(MockitoExtension.class)
public class RemotingServerTlsContextUpdateTest {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private static final String CERT = "/tmp/server.pem";
    private static final String KEY = "/tmp/server.key";

    @Mock
    private MessagingProcessor messagingProcessor;

    @BeforeAll
    static void beforeAll() throws Exception {
        // 初始化配置
        ConfigurationManager.initEnv();
        ConfigurationManager.intConfig();

        // 设置 TLS 相关配置
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        config.setTlsTestModeEnable(true);
        config.setTlsCertPath(CERT);
        config.setTlsKeyPath(KEY);

        // 设置 TLS 系统配置
        TlsSystemConfig.tlsTestModeEnable = config.isTlsTestModeEnable();
        System.setProperty(TlsSystemConfig.TLS_TEST_MODE_ENABLE, Boolean.toString(config.isTlsTestModeEnable()));
        TlsSystemConfig.tlsServerCertPath = config.getTlsCertPath();
        System.setProperty(TlsSystemConfig.TLS_SERVER_CERTPATH, config.getTlsCertPath());
        TlsSystemConfig.tlsServerKeyPath = config.getTlsKeyPath();
        System.setProperty(TlsSystemConfig.TLS_SERVER_KEYPATH, config.getTlsKeyPath());
    }

    @Test
    public void testRemotingServerTlsContextReload() throws Exception {
        // 创建 RemotingProtocolServer 实例
        RemotingProtocolServer remotingProtocolServer = new RemotingProtocolServer(messagingProcessor);

        // 确保服务器实例是 NettyRemotingServer 类型
        Assertions.assertTrue(remotingProtocolServer.defaultRemotingServer instanceof NettyRemotingServer,
            "Default remoting server should be instance of NettyRemotingServer");

        NettyRemotingServer nettyRemotingServer = (NettyRemotingServer) remotingProtocolServer.defaultRemotingServer;

        // 获取 NettyRemotingAbstract 类的 sslContext 字段
        Field sslContextField = NettyRemotingAbstract.class.getDeclaredField("sslContext");
        sslContextField.setAccessible(true);

        // 获取初始的 SSL 上下文
        SslContext originalSslContext = (SslContext) sslContextField.get(nettyRemotingServer);

        // 为了测试，我们需要确保有初始上下文
        if (originalSslContext == null) {
            nettyRemotingServer.loadSslContext();
            originalSslContext = (SslContext) sslContextField.get(nettyRemotingServer);
        }

        // 记录原始的 SSL 上下文
        log.info("Original SSL context: {}", originalSslContext);

        // 触发 TLS 上下文重新加载
        RemotingProtocolServer.RemotingTlsReloadHandler reloadHandler = remotingProtocolServer.tlsReloadHandler;
        reloadHandler.onTlsContextReload();

        // 获取重载后的 SSL 上下文
        SslContext reloadedSslContext = (SslContext) sslContextField.get(nettyRemotingServer);
        log.info("Reloaded SSL context: {}", reloadedSslContext);

        // 验证 SSL 上下文是否已更改
        Assertions.assertNotSame(originalSslContext, reloadedSslContext,
            "SSL context should be replaced after reload");

        // 确保两次都有有效的 SSL 上下文
        if (originalSslContext != null) {
            Assertions.assertNotNull(reloadedSslContext, "Reloaded SSL context should not be null");
        }

        // 清理资源
        remotingProtocolServer.shutdown();
    }

    @Test
    public void testTlsCertificateManagerTriggersCorrectUpdate() throws Exception {
        // 创建 RemotingProtocolServer 实例
        RemotingProtocolServer remotingProtocolServer = new RemotingProtocolServer(messagingProcessor);

        // 获取 NettyRemotingServer 实例
        NettyRemotingServer nettyRemotingServer = (NettyRemotingServer) remotingProtocolServer.defaultRemotingServer;

        // 获取 sslContext 字段
        Field sslContextField = NettyRemotingAbstract.class.getDeclaredField("sslContext");
        sslContextField.setAccessible(true);

        // 获取初始的 SSL 上下文
        SslContext originalSslContext = (SslContext) sslContextField.get(nettyRemotingServer);

        // 为了测试，我们需要确保有初始上下文
        if (originalSslContext == null) {
            nettyRemotingServer.loadSslContext();
            originalSslContext = (SslContext) sslContextField.get(nettyRemotingServer);
        }

        // 通过 TlsCertificateManager 触发重新加载
        TlsCertificateManager tlsCertificateManager = TlsCertificateManager.getInstance();

        // 创建一个监听器列表的副本，以便我们可以直接调用所有监听器
        Field listenersField = TlsCertificateManager.class.getDeclaredField("reloadListeners");
        listenersField.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.List<TlsCertificateManager.TlsContextReloadListener> listeners =
            (java.util.List<TlsCertificateManager.TlsContextReloadListener>) listenersField.get(tlsCertificateManager);

        // 通知所有监听器重新加载
        for (TlsCertificateManager.TlsContextReloadListener listener : new java.util.ArrayList<>(listeners)) {
            listener.onTlsContextReload();
        }

        // 获取重载后的 SSL 上下文
        SslContext reloadedSslContext = (SslContext) sslContextField.get(nettyRemotingServer);

        // 验证 SSL 上下文是否已更改
        Assertions.assertNotSame(originalSslContext, reloadedSslContext,
            "SSL context should be replaced after TlsCertificateManager triggered reload");

        // 清理资源
        remotingProtocolServer.shutdown();
    }
}
